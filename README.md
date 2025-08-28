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
```
  uv run python main.py --env stg --fetch
  uv run python main.py --env stg --org-id 1553 --fetch
  uv run python main.py --env stg --apply 
  uv run python main.py --env stg --restore --backup-timestamp 20250822T181309Z (ls backup directory to get this name easily)
```