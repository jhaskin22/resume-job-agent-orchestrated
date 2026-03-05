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
