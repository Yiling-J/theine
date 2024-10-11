.PHONY: test
test:
	poetry run pytest --benchmark-skip

.PHONY: benchmark
benchmark:
	poetry run pytest --benchmark-only

.PHONY: lint
lint:
	poetry run mypy .

.PHONY: lint-pass
lint-pass:
	poetry run mypy tests/typing/api_pass.py

.PHONY: lint-failed
lint-failed:
	poetry run mypy tests/typing/api_failed.py

trace_bench:
	poetry run python -m benchmarks.trace_bench
