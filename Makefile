.PHONY: ruff mypy lint test

ruff:
	uv run ruff format .
	uv run ruff check --fix .

mypy:
ifdef MYPY_PYTHON_VERSION
	uv run mypy --python-version=$(MYPY_PYTHON_VERSION) .
else
	uv run mypy .
endif

lint: ruff mypy

test:
	uv run pytest
