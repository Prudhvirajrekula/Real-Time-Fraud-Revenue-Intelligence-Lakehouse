#!/usr/bin/env bash
# =============================================================================
# Smoke Test - Verify all platform services are healthy
# =============================================================================
set -euo pipefail

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
RESET='\033[0m'

PASS_COUNT=0
FAIL_COUNT=0
WARN_COUNT=0

pass()  { echo -e "  ${GREEN}[PASS]${RESET} $1"; PASS_COUNT=$((PASS_COUNT + 1)); }
fail()  { echo -e "  ${RED}[FAIL]${RESET} $1"; FAIL_COUNT=$((FAIL_COUNT + 1)); }
warn()  { echo -e "  ${YELLOW}[WARN]${RESET} $1"; WARN_COUNT=$((WARN_COUNT + 1)); }

check_http() {
    local name="$1"
    local url="$2"
    local expected="${3:-200}"
    if curl -sf -o /dev/null -w "%{http_code}" "${url}" 2>/dev/null | grep -q "${expected}"; then
        pass "${name} (${url})"
    else
        fail "${name} (${url})"
    fi
}

check_tcp() {
    local name="$1"
    local host="$2"
    local port="$3"
    if nc -z "${host}" "${port}" 2>/dev/null; then
        pass "${name} (${host}:${port})"
    else
        fail "${name} (${host}:${port})"
    fi
}

echo ""
echo -e "${CYAN}=====================================================${RESET}"
echo -e "${CYAN}  Fraud Intelligence Platform - Smoke Tests${RESET}"
echo -e "${CYAN}=====================================================${RESET}"
echo ""

echo -e "${CYAN}--- Core Infrastructure ---${RESET}"
check_tcp  "Zookeeper"         localhost 2181
check_tcp  "Kafka"             localhost 9092
check_http "Schema Registry"   "http://localhost:8081/subjects"
check_http "Kafka UI"          "http://localhost:8082"
check_http "MinIO API"         "http://localhost:9000/minio/health/live"
check_http "MinIO Console"     "http://localhost:9001"

echo ""
echo -e "${CYAN}--- Processing Layer ---${RESET}"
check_http "Spark Master UI"   "http://localhost:8083"

echo ""
echo -e "${CYAN}--- Orchestration ---${RESET}"
check_http "Airflow Web"       "http://localhost:8086/health"

echo ""
echo -e "${CYAN}--- Analytics & ML ---${RESET}"
check_tcp  "PostgreSQL"        localhost 5432
check_http "ML Server Health"  "http://localhost:8000/health"

echo ""
echo -e "${CYAN}--- Monitoring ---${RESET}"
check_http "Prometheus"        "http://localhost:9090/-/healthy"
check_http "Grafana"           "http://localhost:3000/api/health"

echo ""
echo -e "${CYAN}--- Kafka Topics ---${RESET}"
EXPECTED_TOPICS="orders payments users devices geo_events refunds"
for topic in ${EXPECTED_TOPICS}; do
    if docker exec kafka kafka-topics \
        --bootstrap-server kafka:29092 \
        --describe --topic "${topic}" > /dev/null 2>&1; then
        pass "Kafka topic: ${topic}"
    else
        warn "Kafka topic: ${topic} (may not exist yet)"
    fi
done

echo ""
echo -e "${CYAN}--- MinIO Buckets ---${RESET}"
if docker exec minio sh -c 'mc alias set local http://localhost:9000 $MINIO_ROOT_USER $MINIO_ROOT_PASSWORD > /dev/null 2>&1 && mc ls local/fraud-platform > /dev/null 2>&1'; then
    pass "MinIO bucket: fraud-platform"
else
    warn "MinIO bucket: fraud-platform (not yet initialized)"
fi

echo ""
echo -e "${CYAN}=====================================================${RESET}"
echo -e "PASSED: ${GREEN}${PASS_COUNT}${RESET} | FAILED: ${RED}${FAIL_COUNT}${RESET} | WARNINGS: ${YELLOW}${WARN_COUNT}${RESET}"
echo -e "${CYAN}=====================================================${RESET}"
echo ""

if [ "${FAIL_COUNT}" -gt 0 ]; then
    echo -e "${RED}Some smoke tests failed. Run 'make logs' to investigate.${RESET}"
    exit 1
fi

echo -e "${GREEN}All smoke tests passed! Platform is healthy.${RESET}"
