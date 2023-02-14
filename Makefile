.PHONY: test
test:
	poetry run pytest --benchmark-skip

.PHONY: benchmark
benchmark:
	poetry run pytest --benchmark-only

.PHONY: lint
lint:
	mypy --ignore-missing-imports .
