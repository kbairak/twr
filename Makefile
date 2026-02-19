all: lint test

lint: format check ty

format:
	uv run ruff format

check:
	uv run ruff check --fix

ty:
	uv run ty check

test:
	uv run pytest
