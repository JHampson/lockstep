.PHONY: all test lint format typecheck clean install dev-install help

# Default target
all: lint typecheck test

# Install dependencies
install:
	uv pip install -e .

# Install with dev dependencies
dev-install:
	uv pip install -e ".[dev]"

# Run tests
test:
	pytest tests/ -v

# Run tests with coverage
test-cov:
	pytest tests/ -v --cov=odcs_sync --cov-report=html --cov-report=term

# Lint code
lint:
	ruff check src tests

# Lint and fix
lint-fix:
	ruff check --fix src tests

# Format code
format:
	ruff format src tests

# Check formatting
format-check:
	ruff format --check src tests

# Type checking
typecheck:
	mypy src

# Clean build artifacts
clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	rm -rf src/*.egg-info/
	rm -rf .pytest_cache/
	rm -rf .mypy_cache/
	rm -rf .ruff_cache/
	rm -rf htmlcov/
	rm -rf .coverage
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

# Build package
build: clean
	python -m build

# Show help
help:
	@echo "Available targets:"
	@echo "  all          - Run lint, typecheck, and test"
	@echo "  install      - Install package"
	@echo "  dev-install  - Install package with dev dependencies"
	@echo "  test         - Run tests"
	@echo "  test-cov     - Run tests with coverage"
	@echo "  lint         - Run linter (ruff)"
	@echo "  lint-fix     - Run linter with auto-fix"
	@echo "  format       - Format code (ruff format)"
	@echo "  format-check - Check code formatting"
	@echo "  typecheck    - Run type checker (mypy)"
	@echo "  clean        - Remove build artifacts"
	@echo "  build        - Build package"
	@echo "  help         - Show this help"

