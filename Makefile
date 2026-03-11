.PHONY: help venv install test lint format frontend-install frontend-dev dev

help:
	@echo "MindVault dev shortcuts"
	@echo "  make venv            Create Python venv (.venv)"
	@echo "  make install         Install backend deps (requirements.txt)"
	@echo "  make test            Run pytest"
	@echo "  make lint            Python compile check (fast)"
	@echo "  make frontend-install Install frontend deps"
	@echo "  make frontend-dev    Start frontend dev server"
	@echo "  make dev             Backend sample run + frontend dev hints"

venv:
	python3 -m venv .venv
	@echo "Run: source .venv/bin/activate"

install:
	pip install -r requirements.txt

test:
	pytest -q

lint:
	python3 -m compileall -q .

frontend-install:
	cd frontend && npm install

frontend-dev:
	cd frontend && npm run dev

dev:
	@echo "Backend sample run:"
	@echo "  python3 -m mindvault.runtime.app -w workspace -i sample_data/benchmarks/semi_structured.json"
	@echo "Frontend dev server:"
	@echo "  make frontend-install && make frontend-dev"
