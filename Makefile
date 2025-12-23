.PHONY: ruff mypy lint test

all: lint test

lint: ruff ty mypy

ruff:
	uv run ruff format .
	uv run ruff check --fix .

ty:
	uv run ty check

mypy:
	uv run mypy .

test:
	uv run pytest
