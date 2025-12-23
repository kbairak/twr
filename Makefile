.PHONY: ruff mypy lint test

all: lint test

lint: ruff mypy

ruff:
	uv run ruff format .
	uv run ruff check --fix .

mypy:
ifdef MYPY_PYTHON_VERSION
	uv run mypy --python-version=$(MYPY_PYTHON_VERSION) .
else
	uv run mypy .
endif

test:
	uv run pytest
