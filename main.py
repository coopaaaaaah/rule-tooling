#!/usr/bin/env python3
"""
Convert facts with `sender_receiver` to `perspectives` on rules and write updated content to env file.

- Connects using get_db_connection from helper.py
- Loads rules containing `sender_receiver` in their JSON content
- For each fact:
  - If `sender_receiver` is present:
    - Map values to perspectives:
      * "sender" -> "sender_entity_id"
      * "receiver" -> "receiver_entity_id"
      * "sender_receiver" -> both of the above
    - Merge into existing `perspectives` (if any), de-duplicate
    - Set `fact["type"] = "MULTIPLE_PERSPECTIVES_AGGREGATION"`
- Writes merged output to converted_rules/<env>.json (merged by rule id)
- Can apply env updates back to DB; if rule.status = VALIDATION, update latest rule_validation.rule_content
- Can restore DB contents from backups stored under backups/<env>/<timestamp>/

Usage examples:
  uv run python main.py --env stg --fetch
  uv run python main.py --env stg --org-id 1 --fetch
  uv run python main.py --env stg --apply
  uv run python main.py --env stg --restore --backup-timestamp 20250822T181309Z (ls backup directory to get this name easily)
"""

import argparse
import json
import os
import re
from typing import Any, Dict, List, Optional, Set, Tuple
from datetime import datetime, timezone

from psycopg2.extras import RealDictCursor

from helper import get_db_connection
from config import ENV_MAP

OUTPUT_DIR = "converted_rules"
BACKUP_ROOT = "backups"


def ensure_output_dir() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def ensure_backup_dir(env: str, timestamp: str) -> str:
    path = os.path.join(BACKUP_ROOT, env, timestamp)
    os.makedirs(path, exist_ok=True)
    return path


def save_env_output(env: str, entries: List[Dict[str, Any]]) -> str:
    """Save the rules to a JSON file."""
    rules: Dict[int, Dict[str, Any]] = {}
    for entry in entries:
        rules[int(entry["id"])] = {
            "id": int(entry["id"]),
            "org_id": int(entry["org_id"]),
            "content": entry["content"],
        }
    sorted_rules = list(sorted(rules.values(), key=lambda r: int(r["id"])) )
    out = {
        "env": env,
        "updated_at": datetime.now(timezone.utc).isoformat() + "Z",
        "rules": sorted_rules,
    }
    path = os.path.join(OUTPUT_DIR, f"{env}.json")
    with open(path, "w") as f:
        json.dump(out, f, indent=2, ensure_ascii=False)
    return path


def load_rules(connection, org_id: Optional[int] = None) -> List[Dict[str, Any]]:
    clauses: List[str] = ['rule.content::text ILIKE %s', 'rule.is_synchronous = %s', 'scenario.name = %s']
    params: List[Any] = ['%"sender_receiver"%', False, 'custom_scenario']

    if org_id is not None:
        clauses.append("rule.org_id = %s")
        params.append(org_id)

    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    sql = f"""
        SELECT rule.id, rule.content, rule.org_id
        FROM rule
        JOIN scenario ON scenario.id = rule.scenario_id
        {where_sql}
        ORDER BY id
    """

    with connection.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, params)
        return list(cur.fetchall())


def to_perspective_object(field_name: str) -> Dict[str, str]:
    return {
        "type": "FIELD",
        "field": field_name,
        "model": "txn_event",
        "datatype": "text",
    }


def normalize_sender_receiver(value: Any) -> Optional[str]:
    if not isinstance(value, str):
        return None
    v = value.strip().lower()
    if v in ("sender", "receiver", "sender_receiver"):
        return v
    if v == "senderreceiver" or v == "both" or v == "sender_and_receiver":
        return "sender_receiver"
    if v == "s":
        return "sender"
    if v == "r":
        return "receiver"
    return None


def parse_rule_content(raw_content: Any) -> Optional[Dict[str, Any]]:
    if raw_content is None:
        return None
    if isinstance(raw_content, dict):
        return raw_content
    if isinstance(raw_content, str):
        try:
            return json.loads(raw_content)
        except json.JSONDecodeError:
            try:
                repaired = raw_content.replace("'", '"')
                return json.loads(repaired)
            except Exception:
                return None
    return None


def merge_perspectives(existing: List[Dict[str, str]], additions: List[Dict[str, str]]) -> List[Dict[str, str]]:
    seen: Set[str] = set()
    result: List[Dict[str, str]] = []
    for obj in (existing or []) + additions:
        if not isinstance(obj, dict):
            continue
        field = obj.get("field")
        if not field:
            continue
        key = field.strip().lower()
        if key in seen:
            continue
        seen.add(key)
        result.append(to_perspective_object(field))
    return result


def convert_fact(fact: Dict[str, Any]) -> bool:
    """Convert sender_receiver to perspectives on a single fact. Returns True if updated."""
    if not isinstance(fact, dict):
        return False
    sr_val = fact.get("sender_receiver")
    sr = normalize_sender_receiver(sr_val)
    if not sr:
        return False

    additions: List[Dict[str, str]] = []
    if sr in ("sender", "sender_receiver"):
        additions.append(to_perspective_object("sender_entity_id"))
    if sr in ("receiver", "sender_receiver"):
        additions.append(to_perspective_object("receiver_entity_id"))

    existing = fact.get("perspectives") if isinstance(fact.get("perspectives"), list) else []
    fact["perspectives"] = merge_perspectives(existing, additions)
    fact["type"] = "MULTIPLE_PERSPECTIVES_AGGREGATION"
    return True


def process_rule(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    rule_id = row["id"]
    content = parse_rule_content(row["content"])
    if not content:
        print(f"Rule {rule_id}: unable to parse content; skipping")
        return None

    spec = content.get("specification")
    if not isinstance(spec, dict):
        return None

    facts = spec.get("facts")
    if not isinstance(facts, list):
        return None

    updated = False
    for fact in facts:
        if convert_fact(fact):
            updated = True

    return content if updated else None


def apply_rules_from_env_file(connection, env: str) -> str:
    """Apply rule contents from an env JSON file back into the database with backups.
    If rule.status = 'VALIDATION', updates the latest rule_validation.rule_content instead of rule.content.
    Returns the backup directory path created for this run.
    """
    path = os.path.join(OUTPUT_DIR, f"{env}.json")
    if not os.path.exists(path):
        raise FileNotFoundError(f"Env file not found: {path}")

    with open(path, "r") as f:
        data = json.load(f)

    rules = data.get("rules", []) if isinstance(data, dict) else []
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup_dir = ensure_backup_dir(env, timestamp)

    updated_count = 0
    with connection.cursor(cursor_factory=RealDictCursor) as cur:
        for item in rules:
            try:
                rule_id = int(item["id"]) if "id" in item else None
                org_id = int(item["org_id"]) if "org_id" in item else None
                content = item.get("content")
            except Exception:
                continue
            if rule_id is None or org_id is None or content is None:
                continue

            # Determine rule status
            cur.execute("SELECT status FROM rule WHERE id = %s AND org_id = %s", (rule_id, org_id))
            rule_row = cur.fetchone()
            if not rule_row:
                print(f"Rule {rule_id} (org {org_id}) not found; skipping")
                continue
            status = rule_row.get("status") if isinstance(rule_row, dict) else rule_row[0]

            if status == "VALIDATION":
                # Fetch latest validation record
                cur.execute(
                    """
                    SELECT id, rule_content
                    FROM rule_validation
                    WHERE rule_id = %s
                    ORDER BY created_at DESC NULLS LAST, id DESC
                    LIMIT 1
                    """,
                    (rule_id,),
                )
                rv = cur.fetchone()
                if not rv:
                    print(f"Rule {rule_id} in VALIDATION but no rule_validation row found; skipping")
                    continue
                rv_id = rv.get("id") if isinstance(rv, dict) else rv[0]
                rv_content = rv.get("rule_content") if isinstance(rv, dict) else rv[1]

                # Backup existing validation content
                backup_path = os.path.join(backup_dir, f"rule_{rule_id}_org_{org_id}_validation_{rv_id}_original.json")
                try:
                    if isinstance(rv_content, dict):
                        to_dump = rv_content
                    else:
                        try:
                            to_dump = json.loads(rv_content) if isinstance(rv_content, str) else rv_content
                        except Exception:
                            to_dump = {"raw": rv_content}
                    with open(backup_path, "w") as bf:
                        json.dump(to_dump, bf, indent=2, ensure_ascii=False)
                except Exception as e:
                    print(f"Backup failed for rule_validation {rv_id} (rule {rule_id}): {e}")
                    continue

                # Update validation content
                cur.execute(
                    "UPDATE rule_validation SET rule_content = %s WHERE id = %s",
                    (json.dumps(content), rv_id),
                )
                updated_count += 1
            else:
                # Backup current rule content
                cur.execute("SELECT content FROM rule WHERE id = %s AND org_id = %s", (rule_id, org_id))
                row = cur.fetchone()
                if not row:
                    print(f"Rule {rule_id} (org {org_id}) not found when backing up; skipping")
                    continue

                backup_path = os.path.join(backup_dir, f"rule_{rule_id}_org_{org_id}_original.json")
                try:
                    existing = row["content"] if isinstance(row, dict) else row[0]
                    if isinstance(existing, dict):
                        to_dump = existing
                    else:
                        try:
                            to_dump = json.loads(existing) if isinstance(existing, str) else existing
                        except Exception:
                            to_dump = {"raw": existing}
                    with open(backup_path, "w") as bf:
                        json.dump(to_dump, bf, indent=2, ensure_ascii=False)
                except Exception as e:
                    print(f"Backup failed for rule {rule_id} (org {org_id}): {e}")
                    continue

                # Update rule content
                cur.execute(
                    "UPDATE rule SET content = %s WHERE id = %s AND org_id = %s",
                    (json.dumps(content), rule_id, org_id),
                )
                updated_count += 1

        connection.commit()

    print(f"Applied {updated_count} rule(s) from {path}. Backups at {backup_dir}")
    return backup_dir


def _parse_backup_filename(filename: str) -> Optional[Tuple[str, Dict[str, int]]]:
    """Parse backup filename to determine type and identifiers.
    Returns (kind, ids) where kind is 'rule' or 'validation'.
    """
    m_val = re.match(r"^rule_(\d+)_org_(\d+)_validation_(\d+)_original\.json$", filename)
    if m_val:
        return (
            "validation",
            {"rule_id": int(m_val.group(1)), "org_id": int(m_val.group(2)), "validation_id": int(m_val.group(3))},
        )
    m_rule = re.match(r"^rule_(\d+)_org_(\d+)_original\.json$", filename)
    if m_rule:
        return ("rule", {"rule_id": int(m_rule.group(1)), "org_id": int(m_rule.group(2))})
    return None


def restore_from_backup(connection, env: str, backup_timestamp: Optional[str] = None) -> str:
    """Restore DB contents from backups/<env>/<timestamp>/.
    Returns the directory used.
    """
    if not backup_timestamp:
        raise ValueError("--backup-timestamp is required")
    directory = os.path.join(BACKUP_ROOT, env, backup_timestamp)

    if not os.path.isdir(directory):
        raise FileNotFoundError(f"Backup directory not found: {directory}")

    restored = 0
    files = sorted(f for f in os.listdir(directory) if f.endswith(".json"))

    with connection.cursor(cursor_factory=RealDictCursor) as cur:
        for fname in files:
            parsed = _parse_backup_filename(fname)
            if not parsed:
                continue
            kind, ids = parsed
            rule_id = ids.get("rule_id")
            org_id = ids.get("org_id")

            full_path = os.path.join(directory, fname)
            try:
                with open(full_path, "r") as f:
                    content = json.load(f)
            except Exception as e:
                print(f"Failed to read backup {fname}: {e}")
                continue

            if kind == "validation":
                validation_id = ids["validation_id"]
                cur.execute(
                    "UPDATE rule_validation SET rule_content = %s WHERE id = %s",
                    (json.dumps(content), validation_id),
                )
                restored += 1
            else:
                # rule content
                cur.execute(
                    "UPDATE rule SET content = %s WHERE id = %s AND org_id = %s",
                    (json.dumps(content), rule_id, org_id),
                )
                restored += 1
        connection.commit()

    print(f"Restored {restored} item(s) from {directory}")
    return directory


def main():
    parser = argparse.ArgumentParser(description="Convert sender_receiver to perspectives on rules")
    parser.add_argument("--env", choices=list(ENV_MAP.keys()), default="stg", help="Environment to connect to")
    parser.add_argument("--org-id", type=int, default=None, help="Filter rules by org_id")
    parser.add_argument("--fetch", action="store_true", help="Fetch rules from the database")
    parser.add_argument("--apply", action="store_true", help="Apply rules from env JSON back to the database")
    parser.add_argument("--restore", action="store_true", help="Restore DB contents from backups/<env>/<timestamp>/")
    parser.add_argument("--backup-timestamp", type=str, default=None, help="Timestamp directory under backups/<env>/ to restore from")
    args = parser.parse_args()

    conn = get_db_connection(args.env)

    # Restore path
    if args.restore:
        try:
            directory = restore_from_backup(
                conn,
                args.env,
                backup_timestamp=args.backup_timestamp,
            )
            print(f"Restored from: {directory}")
        finally:
            conn.close()
            print("Database connection closed.")
        return

    # Apply path
    if args.apply:
        try:
            backup_dir = apply_rules_from_env_file(conn, args.env)
            print(f"Backups stored under: {backup_dir}")
        finally:
            conn.close()
            print("Database connection closed.")
        return

    # Fetch path
    if args.fetch:
        try:
            # Remove existing env file before fetching to avoid mixing old data
            try:
                existing_path = os.path.join(OUTPUT_DIR, f"{args.env}.json")
                if os.path.exists(existing_path):
                    os.remove(existing_path)
                    print(f"Deleted existing env file: {existing_path}")
            except Exception as e:
                print(f"Warning: failed to delete existing env file: {e}")

            rows = load_rules(conn, org_id=args.org_id)
            print(f"Loaded {len(rows)} candidate rules")
            aggregated: List[Dict[str, Any]] = []
            converted = 0
            for row in rows:
                updated_content = process_rule(row)
                if updated_content is not None:
                    aggregated.append({"id": row["id"], "org_id": row["org_id"], "content": updated_content})
                    converted += 1
            if aggregated:
                out_path = save_env_output(args.env, aggregated)
                print(f"Wrote {len(aggregated)} updated rule(s) to {out_path}")
            print(f"Done. Converted {converted} of {len(rows)} rules.")
        finally:
            conn.close()
            print("Database connection closed.")


if __name__ == "__main__":
    main()
