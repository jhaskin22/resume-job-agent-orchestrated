# Scaffolded FastAPI App

## Quickstart

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
make test
```

## Layout

- `app/main.py`: FastAPI app entrypoint
- `app/api/`: routers and route modules
- `app/services/`: business logic layer
- `app/core/config.py`: settings
- `tests/`: pytest suite
