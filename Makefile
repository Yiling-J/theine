.PHONY: test
test:
	poetry run pytest --benchmark-skip

.PHONY: benchmark
benchmark:
	poetry run pytest --benchmark-only

.PHONY: lint
lint:
	poetry run mypy --ignore-missing-imports .
trace_bench:
	poetry run python -m benchmarks.trace_bench
