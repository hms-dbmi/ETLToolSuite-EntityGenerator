# ============================================================
# EntityGenerator - Maven Build and Deployment Makefile
# ============================================================
# Usage:
#   make build           Build shaded JARs
#   make test            Run Maven tests
#   make clean           Clean build artifacts
#   make help            Show available targets
# ============================================================

PROJECT_NAME := entity-generator
MVN           := mvn
MAVEN_OPTS    ?= -DskipTests

.DEFAULT_GOAL := help

# ------------------------------------------------------------
# Core Targets
# ------------------------------------------------------------
.PHONY: build
build:
	@echo "Building shaded JARs for $(PROJECT_NAME)..."
	$(MVN) clean package $(MAVEN_OPTS)

.PHONY: test
test:
	@echo "Running unit tests..."
	$(MVN) clean test

.PHONY: deploy
deploy:
	@echo "Deploying $(PROJECT_NAME) to an artifact store?..."

.PHONY: clean
clean:
	@echo "Cleaning build directories..."
	$(MVN) clean

# ------------------------------------------------------------
# Utility Targets
# ------------------------------------------------------------
.PHONY: help
help:
	@echo ""
	@echo "Available Make targets for $(PROJECT_NAME):"
	@echo ""
	@echo "  build          Build shaded JARs locally"
	@echo "  test           Run Maven tests"
	@echo "  clean          Remove build artifacts"
	@echo ""
	@echo "Environment variables:"
	@echo "  MAVEN_OPTS                       Default: -DskipTests"
	@echo ""