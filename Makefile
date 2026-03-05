PYTHON ?= python3

install:
	$(PYTHON) -m pip install -r requirements.txt

test:
	$(PYTHON) -m pytest

coverage:
	$(PYTHON) -m pytest --cov=app --cov-report=term-missing

lint:
	$(PYTHON) -m ruff check app tests
	$(PYTHON) -m black --check app tests
	$(PYTHON) -m mypy app

format:
	$(PYTHON) -m black app tests
	$(PYTHON) -m ruff check --fix app tests

run-backend:
	$(PYTHON) -m uvicorn app.main:app --host 0.0.0.0 --port 18000

run-frontend:
	cd frontend && $(PYTHON) -m http.server 8090 --bind 0.0.0.0
