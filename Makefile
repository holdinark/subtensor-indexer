.PHONY: help build build-fast build-clean up down restart logs health clean prune

# Default target
help:
	@echo "🚀 Subtensor Indexer - Docker Management"
	@echo "==========================================="
	@echo ""
	@echo "Building:"
	@echo "  make build       - Build all services"
	@echo "  make build-fast  - Build all services in parallel"
	@echo "  make build-clean - Clean build (no cache)"
	@echo ""
	@echo "Running:"
	@echo "  make run         - Quick start in development mode (with live reloading)"
	@echo "  make up          - Start all services"
	@echo "  make down        - Stop all services"
	@echo "  make restart     - Restart all services"
	@echo ""
	@echo "Development:"
	@echo "  make dev-mode    - Start with live code reloading (changes reflected instantly)"
	@echo "  make dev-build   - Build services in parallel"
	@echo "  make dev-up      - Start development environment"
	@echo ""
	@echo "Monitoring:"
	@echo "  make logs        - View logs from all services"
	@echo "  make health      - Check health of all services"
	@echo ""
	@echo "Maintenance:"
	@echo "  make clean       - Stop services and remove containers"
	@echo "  make prune       - Clean up Docker system (images, networks, etc.)"
	@echo ""
	@echo "Examples:"
	@echo "  make build SERVICE=shovel_events  - Build specific service"
	@echo "  make logs SERVICE=shovel_events   - View logs for specific service"

# Building
build:
ifdef SERVICE
	./scripts/docker-build.sh $(SERVICE)
else
	./scripts/docker-build.sh --all
endif

build-fast:
ifdef SERVICE
	./scripts/docker-build.sh $(SERVICE)
else
	./scripts/docker-build.sh --all --parallel
endif

build-clean:
ifdef SERVICE
	./scripts/docker-build.sh $(SERVICE) --no-cache
else
	./scripts/docker-build.sh --all --no-cache
endif

# Running services
up:
ifdef SERVICE
	docker compose up -d $(SERVICE)
else
	docker compose up -d
endif

down:
	docker compose down

restart:
ifdef SERVICE
	docker compose restart $(SERVICE)
else
	docker compose down && docker compose up -d
endif

# Monitoring
logs:
ifdef SERVICE
	docker compose logs -f $(SERVICE)
else
	docker compose logs -f
endif

health:
	./scripts/health-check.sh

# Maintenance
clean:
	docker compose down --remove-orphans
	docker compose rm -f

prune:
	docker system prune -f
	docker volume prune -f

# Development helpers
dev-build:
	@echo "🔧 Development build with optimizations..."
	./scripts/docker-build.sh --all --parallel

dev-up:
	@echo "🚀 Starting development environment..."
	docker compose up -d clickhouse
	@echo "⏳ Waiting for ClickHouse to be ready..."
	@sleep 5
	docker compose up -d

dev-logs:
	docker compose logs -f --tail=100

# Development mode with volume mounting (code changes reflected automatically)
dev-mode:
	@echo "🔧 Starting development mode with live code reloading..."
	@echo "📁 Your code changes will be automatically reflected in the containers"
	docker compose up -d clickhouse
	@echo "⏳ Waiting for ClickHouse to be ready..."
	@sleep 5
	docker compose up -d
	@echo ""
	@echo "🎉 Development mode active!"
	@echo "✅ Code changes will be reflected automatically"
	@echo "📝 Edit files and see changes instantly (may need to restart specific services)"
	@echo ""
	@echo "Useful commands:"
	@echo "  make logs                          - View all logs"
	@echo "  make logs SERVICE=shovel_events    - View specific service logs"
	@echo "  docker compose restart SERVICE    - Restart specific service if needed"

# Quick start for new developers
run: dev-mode
	@echo ""
	@echo "🎉 Quick start complete!"
	@echo "✅ All services are running in development mode..."
	@echo ""
	@echo "Run 'make health' to check service status"
	@echo "Run 'make logs' to view all logs"
	@echo "Run 'make logs SERVICE=<name>' to view specific service logs" 