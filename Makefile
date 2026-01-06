.PHONY: help install test lint type-check format clean

all: test lint

install:
	uv sync

test:
	pytest tests
	cd interop && ./run_tests.sh

test-cov:
	pytest tests --cov=capnweb --cov-report=html --cov-report=term


check: lint

lint:
	ruff check
	pyrefly check src
	ruff format --check

format:
	ruff format
	ruff check --fix
	ruff format

clean:
	rm -rf build/ dist/ *.egg-info htmlcov/ .coverage .pytest_cache/ .mypy_cache/ .ruff_cache/
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

publish: clean
	git push --tags
	uv build
	twine upload dist/*
