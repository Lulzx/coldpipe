.PHONY: lint format typecheck test check all

lint:
	uv run ruff check .

format:
	uv run ruff format .

typecheck:
	uv run pyright

test:
	uv run pytest tests/ -v

check: lint typecheck test

all: format check
