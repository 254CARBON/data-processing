# 254Carbon Data Processing Pipeline Makefile
# Provides common development and operations commands

.PHONY: help install start stop restart status logs clean test lint format
.DEFAULT_GOAL := help

# Configuration
PYTHON := python3
PIP := pip3
DOCKER_COMPOSE := docker-compose
VENV_DIR := venv
VENV_PYTHON := $(VENV_DIR)/bin/python
VENV_PIP := $(VENV_DIR)/bin/pip

# Colors for output
RED := \033[0;31m
GREEN := \033[0;32m
YELLOW := \033[0;33m
BLUE := \033[0;34m
NC := \033[0m # No Color

help: ## Show this help message
	@echo "$(BLUE)254Carbon Data Processing Pipeline$(NC)"
	@echo "$(BLUE)================================$(NC)"
	@echo ""
	@echo "$(GREEN)Available commands:$(NC)"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  $(YELLOW)%-20s$(NC) %s\n", $$1, $$2}' $(MAKEFILE_LIST)
	@echo ""
	@echo "$(GREEN)Quick start:$(NC)"
	@echo "  make install    # Install dependencies"
	@echo "  make start      # Start all services"
	@echo "  make test       # Run tests"
	@echo "  make logs       # View logs"
	@echo ""

install: ## Install dependencies and setup environment
	@echo "$(BLUE)Installing dependencies...$(NC)"
	@$(PYTHON) -m venv $(VENV_DIR)
	@$(VENV_PIP) install --upgrade pip
	@$(VENV_PIP) install -r requirements.txt
	@echo "$(GREEN)✅ Dependencies installed$(NC)"

install-dev: ## Install development dependencies
	@echo "$(BLUE)Installing development dependencies...$(NC)"
	@$(VENV_PIP) install -r requirements-dev.txt
	@echo "$(GREEN)✅ Development dependencies installed$(NC)"

start: ## Start all services
	@echo "$(BLUE)Starting all services...$(NC)"
	@$(DOCKER_COMPOSE) up -d
	@echo "$(GREEN)✅ All services started$(NC)"
	@echo "$(YELLOW)Services available at:$(NC)"
	@echo "  Normalization: http://localhost:8080"
	@echo "  NormalizeTicks: http://localhost:8088"
	@echo "  Normalization:  http://localhost:8080"
	@echo "  Enrichment:     http://localhost:8081"
	@echo "  Aggregation:    http://localhost:8082"
	@echo "  Projection:     http://localhost:8083"
	@echo "  Prometheus:    http://localhost:9090"
	@echo "  Grafana:       http://localhost:3000 (admin/admin)"

start-infra: ## Start only infrastructure services
	@echo "$(BLUE)Starting infrastructure services...$(NC)"
	@$(DOCKER_COMPOSE) up -d kafka zookeeper clickhouse postgres redis
	@echo "$(GREEN)✅ Infrastructure services started$(NC)"

start-services: ## Start only application services
	@echo "$(BLUE)Starting application services...$(NC)"
	@$(DOCKER_COMPOSE) up -d normalize-ticks-service normalization-service enrichment-service aggregation-service projection-service
	@echo "$(GREEN)✅ Application services started$(NC)"

start-monitoring: ## Start monitoring services
	@echo "$(BLUE)Starting monitoring services...$(NC)"
	@$(DOCKER_COMPOSE) up -d prometheus grafana node-exporter cadvisor
	@echo "$(GREEN)✅ Monitoring services started$(NC)"

stop: ## Stop all services
	@echo "$(BLUE)Stopping all services...$(NC)"
	@$(DOCKER_COMPOSE) down
	@echo "$(GREEN)✅ All services stopped$(NC)"

restart: ## Restart all services
	@echo "$(BLUE)Restarting all services...$(NC)"
	@$(DOCKER_COMPOSE) restart
	@echo "$(GREEN)✅ All services restarted$(NC)"

status: ## Show service status
	@echo "$(BLUE)Service Status:$(NC)"
	@$(DOCKER_COMPOSE) ps
	@echo ""
	@echo "$(BLUE)Health Checks:$(NC)"
	@echo -n "NormalizeTicks: "
	@curl -s http://localhost:8088/health > /dev/null && echo "$(GREEN)✅ Healthy$(NC)" || echo "$(RED)❌ Unhealthy$(NC)"
	@echo -n "Normalization: "
	@curl -s http://localhost:8080/health > /dev/null && echo "$(GREEN)✅ Healthy$(NC)" || echo "$(RED)❌ Unhealthy$(NC)"
	@echo -n "Enrichment:    "
	@curl -s http://localhost:8081/health > /dev/null && echo "$(GREEN)✅ Healthy$(NC)" || echo "$(RED)❌ Unhealthy$(NC)"
	@echo -n "Aggregation:   "
	@curl -s http://localhost:8082/health > /dev/null && echo "$(GREEN)✅ Healthy$(NC)" || echo "$(RED)❌ Unhealthy$(NC)"
	@echo -n "Projection:    "
	@curl -s http://localhost:8083/health > /dev/null && echo "$(GREEN)✅ Healthy$(NC)" || echo "$(RED)❌ Unhealthy$(NC)"

logs: ## Show logs for all services
	@echo "$(BLUE)Showing logs for all services...$(NC)"
	@$(DOCKER_COMPOSE) logs -f

logs-normalize-ticks: ## Show logs for normalize-ticks service
	@$(DOCKER_COMPOSE) logs -f normalize-ticks-service

logs-normalization: ## Show logs for normalization service
	@$(DOCKER_COMPOSE) logs -f normalization-service

logs-enrichment: ## Show logs for enrichment service
	@$(DOCKER_COMPOSE) logs -f enrichment-service

logs-aggregation: ## Show logs for aggregation service
	@$(DOCKER_COMPOSE) logs -f aggregation-service

logs-projection: ## Show logs for projection service
	@$(DOCKER_COMPOSE) logs -f projection-service

logs-kafka: ## Show logs for Kafka
	@$(DOCKER_COMPOSE) logs -f kafka

logs-clickhouse: ## Show logs for ClickHouse
	@$(DOCKER_COMPOSE) logs -f clickhouse

logs-postgres: ## Show logs for PostgreSQL
	@$(DOCKER_COMPOSE) logs -f postgres

logs-redis: ## Show logs for Redis
	@$(DOCKER_COMPOSE) logs -f redis

logs-monitoring: ## Show logs for monitoring services
	@$(DOCKER_COMPOSE) logs -f prometheus grafana

clean: ## Clean up containers, volumes, and images
	@echo "$(BLUE)Cleaning up...$(NC)"
	@$(DOCKER_COMPOSE) down -v --remove-orphans
	@docker system prune -f
	@echo "$(GREEN)✅ Cleanup complete$(NC)"

clean-all: ## Clean up everything including images
	@echo "$(BLUE)Cleaning up everything...$(NC)"
	@$(DOCKER_COMPOSE) down -v --remove-orphans
	@docker system prune -af
	@docker volume prune -f
	@echo "$(GREEN)✅ Complete cleanup done$(NC)"

test: ## Run all tests
	@echo "$(BLUE)Running tests...$(NC)"
	@$(VENV_PYTHON) scripts/test_integration_simple.py
	@$(VENV_PYTHON) scripts/test_monitoring.py
	@echo "$(GREEN)✅ All tests passed$(NC)"

test-unit: ## Run unit tests
	@echo "$(BLUE)Running unit tests...$(NC)"
	@$(VENV_PYTHON) -m pytest tests/unit/ -v
	@echo "$(GREEN)✅ Unit tests passed$(NC)"

test-integration: ## Run integration tests
	@echo "$(BLUE)Running integration tests...$(NC)"
	@$(VENV_PYTHON) scripts/test_integration_simple.py
	@echo "$(GREEN)✅ Integration tests passed$(NC)"

test-monitoring: ## Run monitoring tests
	@echo "$(BLUE)Running monitoring tests...$(NC)"
	@$(VENV_PYTHON) scripts/test_monitoring.py
	@echo "$(GREEN)✅ Monitoring tests passed$(NC)"

lint: ## Run linting
	@echo "$(BLUE)Running linting...$(NC)"
	@$(VENV_PYTHON) -m flake8 --max-line-length=100 --ignore=E203,W503 .
	@$(VENV_PYTHON) -m mypy --ignore-missing-imports .
	@echo "$(GREEN)✅ Linting passed$(NC)"

format: ## Format code
	@echo "$(BLUE)Formatting code...$(NC)"
	@$(VENV_PYTHON) -m black --line-length=100 .
	@$(VENV_PYTHON) -m isort .
	@echo "$(GREEN)✅ Code formatted$(NC)"

generate-data: ## Generate sample data
	@echo "$(BLUE)Generating sample data...$(NC)"
	@$(VENV_PYTHON) scripts/generate_sample_data.py --mode continuous --count 100
	@echo "$(GREEN)✅ Sample data generated$(NC)"

generate-historical: ## Generate historical data
	@echo "$(BLUE)Generating historical data...$(NC)"
	@$(VENV_PYTHON) scripts/generate_sample_data.py --mode historical --days 7
	@echo "$(GREEN)✅ Historical data generated$(NC)"

generate-high-volume: ## Generate high-volume test data
	@echo "$(BLUE)Generating high-volume test data...$(NC)"
	@$(VENV_PYTHON) scripts/generate_sample_data.py --mode high-volume --count 10000
	@echo "$(GREEN)✅ High-volume data generated$(NC)"

setup-monitoring: ## Setup monitoring infrastructure
	@echo "$(BLUE)Setting up monitoring...$(NC)"
	@$(VENV_PYTHON) scripts/setup_monitoring.py
	@echo "$(GREEN)✅ Monitoring setup complete$(NC)"

check-health: ## Check health of all services
	@echo "$(BLUE)Checking service health...$(NC)"
	@echo "Normalization Service:"
	@curl -s http://localhost:8080/health | jq '.' || echo "$(RED)Service not responding$(NC)"
	@echo ""
	@echo "Enrichment Service:"
	@curl -s http://localhost:8081/health | jq '.' || echo "$(RED)Service not responding$(NC)"
	@echo ""
	@echo "Aggregation Service:"
	@curl -s http://localhost:8082/health | jq '.' || echo "$(RED)Service not responding$(NC)"
	@echo ""
	@echo "Projection Service:"
	@curl -s http://localhost:8083/health | jq '.' || echo "$(RED)Service not responding$(NC)"

check-metrics: ## Check metrics endpoints
	@echo "$(BLUE)Checking metrics...$(NC)"
	@echo "Normalization Service Metrics:"
	@curl -s http://localhost:8080/metrics | head -10
	@echo ""
	@echo "Prometheus Targets:"
	@curl -s http://localhost:9090/api/v1/targets | jq '.data.activeTargets[] | {job: .labels.job, health: .health}' || echo "$(RED)Prometheus not responding$(NC)"

backup: ## Backup data and configuration
	@echo "$(BLUE)Creating backup...$(NC)"
	@mkdir -p backups/$(shell date +%Y%m%d_%H%M%S)
	@$(DOCKER_COMPOSE) exec clickhouse clickhouse-backup create backup_$(shell date +%Y%m%d_%H%M%S) || echo "$(YELLOW)ClickHouse backup failed$(NC)"
	@$(DOCKER_COMPOSE) exec postgres pg_dump -U user -d db > backups/$(shell date +%Y%m%d_%H%M%S)/postgres_backup.sql || echo "$(YELLOW)PostgreSQL backup failed$(NC)"
	@echo "$(GREEN)✅ Backup created$(NC)"

restore: ## Restore from backup
	@echo "$(BLUE)Restoring from backup...$(NC)"
	@echo "$(YELLOW)Please specify backup directory: make restore BACKUP_DIR=backups/20240101_120000$(NC)"
	@if [ -z "$(BACKUP_DIR)" ]; then echo "$(RED)Error: BACKUP_DIR not specified$(NC)"; exit 1; fi
	@$(DOCKER_COMPOSE) exec clickhouse clickhouse-backup restore $(BACKUP_DIR) || echo "$(YELLOW)ClickHouse restore failed$(NC)"
	@$(DOCKER_COMPOSE) exec postgres psql -U user -d db < $(BACKUP_DIR)/postgres_backup.sql || echo "$(YELLOW)PostgreSQL restore failed$(NC)"
	@echo "$(GREEN)✅ Restore completed$(NC)"

scale: ## Scale services
	@echo "$(BLUE)Scaling services...$(NC)"
	@echo "$(YELLOW)Usage: make scale SERVICE=normalization-service REPLICAS=3$(NC)"
	@if [ -z "$(SERVICE)" ] || [ -z "$(REPLICAS)" ]; then echo "$(RED)Error: SERVICE and REPLICAS must be specified$(NC)"; exit 1; fi
	@$(DOCKER_COMPOSE) up -d --scale $(SERVICE)=$(REPLICAS)
	@echo "$(GREEN)✅ $(SERVICE) scaled to $(REPLICAS) replicas$(NC)"

update: ## Update services to latest version
	@echo "$(BLUE)Updating services...$(NC)"
	@$(DOCKER_COMPOSE) pull
	@$(DOCKER_COMPOSE) up -d
	@echo "$(GREEN)✅ Services updated$(NC)"

security-scan: ## Run security scan
	@echo "$(BLUE)Running security scan...$(NC)"
	@$(VENV_PYTHON) -m safety check
	@$(VENV_PYTHON) -m bandit -r . -f json -o security-report.json || echo "$(YELLOW)Security issues found$(NC)"
	@echo "$(GREEN)✅ Security scan completed$(NC)"

performance-test: ## Run performance tests
	@echo "$(BLUE)Running performance tests...$(NC)"
	@$(VENV_PYTHON) scripts/generate_sample_data.py --mode high-volume --count 50000
	@echo "$(GREEN)✅ Performance test completed$(NC)"

docs: ## Generate documentation
	@echo "$(BLUE)Generating documentation...$(NC)"
	@$(VENV_PYTHON) -m pydoc -w service-normalization.app.main
	@$(VENV_PYTHON) -m pydoc -w service-enrichment.app.main
	@$(VENV_PYTHON) -m pydoc -w service-aggregation.app.main
	@$(VENV_PYTHON) -m pydoc -w service-projection.app.main
	@echo "$(GREEN)✅ Documentation generated$(NC)"

dev-setup: ## Setup development environment
	@echo "$(BLUE)Setting up development environment...$(NC)"
	@make install
	@make install-dev
	@make start-infra
	@make setup-monitoring
	@echo "$(GREEN)✅ Development environment ready$(NC)"
	@echo "$(YELLOW)Next steps:$(NC)"
	@echo "  1. make start-services"
	@echo "  2. make test"
	@echo "  3. make generate-data"

prod-setup: ## Setup production environment
	@echo "$(BLUE)Setting up production environment...$(NC)"
	@echo "$(YELLOW)This will set up production-ready configuration$(NC)"
	@cp docker-compose.yml docker-compose.prod.yml
	@sed -i 's/DATA_PROC_ENV=local/DATA_PROC_ENV=production/g' docker-compose.prod.yml
	@sed -i 's/DATA_PROC_LOG_LEVEL=info/DATA_PROC_LOG_LEVEL=warning/g' docker-compose.prod.yml
	@echo "$(GREEN)✅ Production environment configured$(NC)"
	@echo "$(YELLOW)Use: docker-compose -f docker-compose.prod.yml up -d$(NC)"

# Service-specific commands
start-normalization: ## Start only normalization service
	@$(DOCKER_COMPOSE) up -d normalization-service

start-normalize-ticks: ## Start only normalize-ticks service
	@$(DOCKER_COMPOSE) up -d normalize-ticks-service

start-enrichment: ## Start only enrichment service
	@$(DOCKER_COMPOSE) up -d enrichment-service

start-aggregation: ## Start only aggregation service
	@$(DOCKER_COMPOSE) up -d aggregation-service

start-projection: ## Start only projection service
	@$(DOCKER_COMPOSE) up -d projection-service

restart-normalization: ## Restart normalization service
	@$(DOCKER_COMPOSE) restart normalization-service

restart-normalize-ticks: ## Restart normalize-ticks service
	@$(DOCKER_COMPOSE) restart normalize-ticks-service

restart-enrichment: ## Restart enrichment service
	@$(DOCKER_COMPOSE) restart enrichment-service

restart-aggregation: ## Restart aggregation service
	@$(DOCKER_COMPOSE) restart aggregation-service

restart-projection: ## Restart projection service
	@$(DOCKER_COMPOSE) restart projection-service

# Database commands
db-migrate: ## Run database migrations
	@echo "$(BLUE)Running database migrations...$(NC)"
	@$(VENV_PYTHON) migrations/clickhouse/migrate.py
	@$(VENV_PYTHON) migrations/postgres/migrate.py
	@echo "$(GREEN)✅ Database migrations completed$(NC)"

db-reset: ## Reset databases
	@echo "$(BLUE)Resetting databases...$(NC)"
	@$(DOCKER_COMPOSE) down -v
	@$(DOCKER_COMPOSE) up -d clickhouse postgres redis
	@sleep 10
	@make db-migrate
	@echo "$(GREEN)✅ Databases reset$(NC)"

# Monitoring commands
monitoring-status: ## Check monitoring status
	@echo "$(BLUE)Monitoring Status:$(NC)"
	@echo -n "Prometheus: "
	@curl -s http://localhost:9090/-/healthy > /dev/null && echo "$(GREEN)✅ Healthy$(NC)" || echo "$(RED)❌ Unhealthy$(NC)"
	@echo -n "Grafana:    "
	@curl -s http://localhost:3000/api/health > /dev/null && echo "$(GREEN)✅ Healthy$(NC)" || echo "$(RED)❌ Unhealthy$(NC)"

monitoring-dashboards: ## Open monitoring dashboards
	@echo "$(BLUE)Opening monitoring dashboards...$(NC)"
	@echo "Prometheus: http://localhost:9090"
	@echo "Grafana: http://localhost:3000 (admin/admin)"
	@open http://localhost:9090 || echo "$(YELLOW)Please open http://localhost:9090 manually$(NC)"
	@open http://localhost:3000 || echo "$(YELLOW)Please open http://localhost:3000 manually$(NC)"

# Utility commands
shell-normalization: ## Open shell in normalization service container
	@$(DOCKER_COMPOSE) exec normalization-service /bin/bash

shell-enrichment: ## Open shell in enrichment service container
	@$(DOCKER_COMPOSE) exec enrichment-service /bin/bash

shell-aggregation: ## Open shell in aggregation service container
	@$(DOCKER_COMPOSE) exec aggregation-service /bin/bash

shell-projection: ## Open shell in projection service container
	@$(DOCKER_COMPOSE) exec projection-service /bin/bash

shell-clickhouse: ## Open ClickHouse client
	@$(DOCKER_COMPOSE) exec clickhouse clickhouse-client

shell-postgres: ## Open PostgreSQL client
	@$(DOCKER_COMPOSE) exec postgres psql -U user -d db

shell-redis: ## Open Redis client
	@$(DOCKER_COMPOSE) exec redis redis-cli

# Helm commands
HELM := helm
NAMESPACE := data-processing
RELEASE_NAME := data-processing-pipeline

helm-install: ## Install Helm chart
	@echo "$(BLUE)Installing Helm chart...$(NC)"
	@$(HELM) upgrade --install $(RELEASE_NAME) ./helm \
		--namespace $(NAMESPACE) \
		--create-namespace \
		--wait
	@echo "$(GREEN)✅ Helm chart installed$(NC)"

helm-upgrade: ## Upgrade Helm chart
	@echo "$(BLUE)Upgrading Helm chart...$(NC)"
	@$(HELM) upgrade $(RELEASE_NAME) ./helm \
		--namespace $(NAMESPACE) \
		--wait
	@echo "$(GREEN)✅ Helm chart upgraded$(NC)"

helm-uninstall: ## Uninstall Helm chart
	@echo "$(BLUE)Uninstalling Helm chart...$(NC)"
	@$(HELM) uninstall $(RELEASE_NAME) --namespace $(NAMESPACE)
	@echo "$(GREEN)✅ Helm chart uninstalled$(NC)"

helm-status: ## Show Helm chart status
	@echo "$(BLUE)Helm chart status:$(NC)"
	@$(HELM) status $(RELEASE_NAME) --namespace $(NAMESPACE)

helm-history: ## Show Helm chart history
	@echo "$(BLUE)Helm chart history:$(NC)"
	@$(HELM) history $(RELEASE_NAME) --namespace $(NAMESPACE)

helm-rollback: ## Rollback Helm chart
	@echo "$(BLUE)Rolling back Helm chart...$(NC)"
	@$(HELM) rollback $(RELEASE_NAME) --namespace $(NAMESPACE)
	@echo "$(GREEN)✅ Helm chart rolled back$(NC)"

helm-template: ## Generate Helm templates
	@echo "$(BLUE)Generating Helm templates...$(NC)"
	@$(HELM) template $(RELEASE_NAME) ./helm --namespace $(NAMESPACE)

helm-lint: ## Lint Helm chart
	@echo "$(BLUE)Linting Helm chart...$(NC)"
	@$(HELM) lint ./helm
	@echo "$(GREEN)✅ Helm chart linted$(NC)"

helm-dry-run: ## Dry run Helm chart installation
	@echo "$(BLUE)Dry running Helm chart installation...$(NC)"
	@$(HELM) upgrade --install $(RELEASE_NAME) ./helm \
		--namespace $(NAMESPACE) \
		--create-namespace \
		--dry-run --debug

helm-values: ## Show Helm chart values
	@echo "$(BLUE)Helm chart values:$(NC)"
	@$(HELM) get values $(RELEASE_NAME) --namespace $(NAMESPACE)

helm-manifest: ## Show Helm chart manifest
	@echo "$(BLUE)Helm chart manifest:$(NC)"
	@$(HELM) get manifest $(RELEASE_NAME) --namespace $(NAMESPACE)

helm-dependencies: ## Update Helm chart dependencies
	@echo "$(BLUE)Updating Helm chart dependencies...$(NC)"
	@$(HELM) dependency update ./helm
	@echo "$(GREEN)✅ Helm chart dependencies updated$(NC)"

helm-package: ## Package Helm chart
	@echo "$(BLUE)Packaging Helm chart...$(NC)"
	@$(HELM) package ./helm
	@echo "$(GREEN)✅ Helm chart packaged$(NC)"

# Certificate management
certs-generate: ## Generate mTLS certificates
	@echo "$(BLUE)Generating mTLS certificates...$(NC)"
	@./scripts/generate-ca-certs.sh
	@echo "$(GREEN)✅ mTLS certificates generated$(NC)"

certs-validate: ## Validate certificates
	@echo "$(BLUE)Validating certificates...$(NC)"
	@openssl x509 -in certs/ca-cert.pem -text -noout
	@echo "$(GREEN)✅ Certificates validated$(NC)"

certs-clean: ## Clean certificates
	@echo "$(BLUE)Cleaning certificates...$(NC)"
	@rm -rf certs/
	@echo "$(GREEN)✅ Certificates cleaned$(NC)"

# Kubernetes commands
k8s-apply: ## Apply Kubernetes manifests
	@echo "$(BLUE)Applying Kubernetes manifests...$(NC)"
	@kubectl apply -f helm/templates/
	@echo "$(GREEN)✅ Kubernetes manifests applied$(NC)"

k8s-delete: ## Delete Kubernetes manifests
	@echo "$(BLUE)Deleting Kubernetes manifests...$(NC)"
	@kubectl delete -f helm/templates/
	@echo "$(GREEN)✅ Kubernetes manifests deleted$(NC)"

k8s-status: ## Show Kubernetes status
	@echo "$(BLUE)Kubernetes status:$(NC)"
	@kubectl get pods -n $(NAMESPACE)
	@kubectl get services -n $(NAMESPACE)
	@kubectl get deployments -n $(NAMESPACE)

k8s-logs: ## Show Kubernetes logs
	@echo "$(BLUE)Kubernetes logs:$(NC)"
	@kubectl logs -f -l app.kubernetes.io/name=data-processing-pipeline -n $(NAMESPACE)

k8s-describe: ## Describe Kubernetes resources
	@echo "$(BLUE)Describing Kubernetes resources:$(NC)"
	@kubectl describe pods -n $(NAMESPACE)
	@kubectl describe services -n $(NAMESPACE)

k8s-port-forward: ## Port forward services
	@echo "$(BLUE)Port forwarding services...$(NC)"
	@echo "Normalization: kubectl port-forward svc/$(RELEASE_NAME)-normalization-service 8080:8080 -n $(NAMESPACE)"
	@echo "Enrichment: kubectl port-forward svc/$(RELEASE_NAME)-enrichment-service 8081:8081 -n $(NAMESPACE)"
	@echo "Aggregation: kubectl port-forward svc/$(RELEASE_NAME)-aggregation-service 8082:8082 -n $(NAMESPACE)"
	@echo "Projection: kubectl port-forward svc/$(RELEASE_NAME)-projection-service 8083:8083 -n $(NAMESPACE)"

# Production deployment
prod-deploy: ## Deploy to production
	@echo "$(BLUE)Deploying to production...$(NC)"
	@make helm-dependencies
	@make helm-lint
	@make helm-install
	@echo "$(GREEN)✅ Production deployment completed$(NC)"

prod-rollback: ## Rollback production deployment
	@echo "$(BLUE)Rolling back production deployment...$(NC)"
	@make helm-rollback
	@echo "$(GREEN)✅ Production rollback completed$(NC)"

# Version information
version: ## Show version information
	@echo "$(BLUE)254Carbon Data Processing Pipeline$(NC)"
	@echo "$(BLUE)Version: 1.0.0$(NC)"
	@echo "$(BLUE)Python: $(shell $(PYTHON) --version)$(NC)"
	@echo "$(BLUE)Docker: $(shell docker --version)$(NC)"
	@echo "$(BLUE)Docker Compose: $(shell $(DOCKER_COMPOSE) --version)$(NC)"
	@echo "$(BLUE)Helm: $(shell $(HELM) version --short)$(NC)"
	@echo "$(BLUE)Kubectl: $(shell kubectl version --client --short)$(NC)"
