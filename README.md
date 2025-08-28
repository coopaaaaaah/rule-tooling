# Rule Tooling

This tool will convert DMB rules with EVENT_BY_OBJECT_FACTS `sender_receiver` props to `perspectives`. 

# Basic flow for rule migrations
<img width="334" height="104" alt="image" src="https://github.com/user-attachments/assets/06b68a88-90a1-4ae2-973a-98c44597d9d6" />

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
```

Fetch by specific org (expected future use)
```
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
