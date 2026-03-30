.PHONY: help install dev backend frontend test clean docker-up docker-down

help:
	@echo "InsightFlow AI - Available Commands"
	@echo "====================================="
	@echo "make install    - Install all dependencies"
	@echo "make dev       - Run both services in development"
	@echo "make backend   - Run backend only"
	@echo "make frontend  - Run frontend only"
	@echo "make test      - Run all tests"
	@echo "make clean     - Clean cache files"
	@echo "make docker-up - Start all services with Docker"
	@echo "make docker-down - Stop all Docker services"

install:
	cd frontend && npm install
	cd backend && python -m venv .venv && .venv/Scripts/activate && pip install -r requirements.txt

dev:
	@echo "Starting both services..."
	@ concurrently "npm run dev" "cd ../backend && .venv/Scripts/activate && uvicorn app.main:app --reload"

backend:
	cd backend && .venv/Scripts/activate && uvicorn app.main:app --reload

frontend:
	cd frontend && npm run dev

test:
	cd backend && pytest
	cd frontend && npm run test

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name "node_modules" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	cd frontend && rm -rf dist

docker-up:
	docker compose up --build

docker-down:
	docker compose down

docker-logs:
	docker compose logs -f
