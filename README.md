# Rule Tooling

# Requirements
- Have `uv` installed
- Have python (at least 3.12) installed
- Have DB access, get through pacman and supply the username / password in the config.py file

# Setup
```
cp .config-template .config.py
uv venv 
uv sync 
```

# Commands (while in uv venv)

This will create a local-only directory called "converted_rules" with the "env".json file of the rules that were converted. This is a time to review changes. 
```
  uv run python main.py --env stg --fetch
  uv run python main.py --env stg --org-id 1553 --fetch
```

This requires fetch. It applies the changes inside of the fetch to the latest rule content. This also creates a backup of the existing rules before it saves the new content.
```
  uv run python main.py --env stg --apply 
```

Made a mistake? Restore from a backup.
```
  uv run python main.py --env stg --restore --backup-timestamp 20250822T181309Z (ls backup directory to get this name easily)
```