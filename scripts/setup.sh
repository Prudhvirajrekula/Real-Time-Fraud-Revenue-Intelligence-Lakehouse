#!/usr/bin/env bash
# =============================================================================
# Full Platform Setup Script
# =============================================================================
set -euo pipefail

GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
RESET='\033[0m'

echo -e "${BLUE}"
echo "╔═══════════════════════════════════════════════════════════╗"
echo "║  Fraud & Revenue Intelligence Platform Setup              ║"
echo "║  FAANG-Grade Local Data Engineering Platform              ║"
echo "╚═══════════════════════════════════════════════════════════╝"
echo -e "${RESET}"

# Prerequisites check
echo -e "${BLUE}[1/6] Checking prerequisites...${RESET}"
for cmd in docker docker-compose python3 curl; do
    if command -v "${cmd}" > /dev/null 2>&1; then
        echo -e "  ${GREEN}✓ ${cmd}${RESET}"
    else
        echo -e "  ${RED}✗ ${cmd} not found${RESET}"
        exit 1
    fi
done

DOCKER_VERSION=$(docker --version | grep -oP '\d+\.\d+')
echo -e "  ${GREEN}✓ Docker ${DOCKER_VERSION}${RESET}"

# Check available memory
AVAILABLE_MEM_GB=$(python3 -c "import os; print(round(os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES') / 1024**3, 1))" 2>/dev/null || echo "unknown")
echo -e "  ${YELLOW}ℹ Available RAM: ${AVAILABLE_MEM_GB}GB (need ~12GB)${RESET}"

# Setup Python dependencies (for local unit tests)
echo ""
echo -e "${BLUE}[2/6] Installing Python test dependencies...${RESET}"
pip3 install --quiet \
    pytest pytest-cov pytest-asyncio \
    faker numpy scipy structlog \
    xgboost scikit-learn imbalanced-learn shap optuna \
    psycopg2-binary sqlalchemy \
    confluent-kafka \
    boto3 \
    fastapi httpx \
    2>/dev/null || echo -e "  ${YELLOW}Warning: some packages failed to install locally (OK for Docker)${RESET}"

# Create required local directories
echo ""
echo -e "${BLUE}[3/6] Creating directory structure...${RESET}"
dirs=(
    "airflow/logs"
    "airflow/plugins"
    "data_quality/validations"
    "data_quality/expectations"
    "data_quality/data_docs"
    "ml/models/registry"
    "tests/unit"
    "tests/integration"
)
for dir in "${dirs[@]}"; do
    mkdir -p "${dir}"
    echo -e "  ${GREEN}✓ ${dir}${RESET}"
done

# Ensure scripts are executable
chmod +x scripts/*.sh 2>/dev/null || true

# Copy env if not exists
if [ ! -f .env ]; then
    echo -e "${YELLOW}Warning: .env not found. Using defaults.${RESET}"
fi

# Build Docker images
echo ""
echo -e "${BLUE}[4/6] Building Docker images...${RESET}"
docker compose build --parallel

# Start services
echo ""
echo -e "${BLUE}[5/6] Starting services...${RESET}"
docker compose up -d

echo ""
echo -e "${YELLOW}Waiting for services to initialize (60s)...${RESET}"
sleep 60

# Initialize platform
echo ""
echo -e "${BLUE}[6/6] Initializing platform...${RESET}"

echo "  Creating Kafka topics..."
bash scripts/create_kafka_topics.sh || echo -e "  ${YELLOW}Will retry - Kafka may still be starting${RESET}"

echo "  MinIO buckets being initialized by minio-init container..."

echo ""
echo -e "${GREEN}╔═══════════════════════════════════════════════════════════╗${RESET}"
echo -e "${GREEN}║  Platform setup complete!                                  ║${RESET}"
echo -e "${GREEN}╠═══════════════════════════════════════════════════════════╣${RESET}"
echo -e "${GREEN}║  Kafka UI:       http://localhost:8082                     ║${RESET}"
echo -e "${GREEN}║  Airflow:        http://localhost:8086  (admin/admin123)   ║${RESET}"
echo -e "${GREEN}║  MinIO Console:  http://localhost:9001  (admin/admin123)   ║${RESET}"
echo -e "${GREEN}║  Spark UI:       http://localhost:8083                     ║${RESET}"
echo -e "${GREEN}║  Grafana:        http://localhost:3000  (admin/grafana123) ║${RESET}"
echo -e "${GREEN}║  ML API Docs:    http://localhost:8000/docs                ║${RESET}"
echo -e "${GREEN}║  Prometheus:     http://localhost:9090                     ║${RESET}"
echo -e "${GREEN}╠═══════════════════════════════════════════════════════════╣${RESET}"
echo -e "${GREEN}║  Run 'make smoke-test' to verify health                    ║${RESET}"
echo -e "${GREEN}╚═══════════════════════════════════════════════════════════╝${RESET}"
