.PHONY: test
test:
	uv run pytest --benchmark-skip

.PHONY: benchmark
benchmark:
	uv run pytest --benchmark-only

.PHONY: lint
lint:
	uv run mypy .

.PHONY: lint-pass
lint-pass:
	uv run mypy tests/typing/api_pass.py

.PHONY: lint-failed
lint-failed:
	uv run mypy tests/typing/api_failed.py

trace_bench:
	uv run python -m benchmarks.trace_bench
