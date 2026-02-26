#!/usr/bin/env bash
# =============================================================================
# Create Kafka Topics for the Fraud Intelligence Platform
# =============================================================================
set -euo pipefail

KAFKA_CONTAINER="${KAFKA_CONTAINER:-kafka}"
BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-kafka:29092}"
REPLICATION_FACTOR="${KAFKA_REPLICATION_FACTOR:-1}"

BLUE='\033[0;34m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RESET='\033[0m'

# Topic definitions: "name partitions retention_ms"
TOPICS=(
    "orders 6 604800000"
    "payments 6 604800000"
    "users 3 2592000000"
    "devices 6 604800000"
    "geo_events 6 259200000"
    "refunds 3 2592000000"
)

echo -e "${BLUE}Creating Kafka topics...${RESET}"
echo -e "  Bootstrap: ${BOOTSTRAP_SERVERS}"
echo -e "  Replication: ${REPLICATION_FACTOR}"
echo ""

for entry in "${TOPICS[@]}"; do
    read -r topic partitions retention <<< "${entry}"

    echo -e "  Creating topic: ${YELLOW}${topic}${RESET} (partitions=${partitions}, retention=${retention}ms)"

    docker exec "${KAFKA_CONTAINER}" kafka-topics \
        --bootstrap-server "${BOOTSTRAP_SERVERS}" \
        --create \
        --if-not-exists \
        --topic "${topic}" \
        --partitions "${partitions}" \
        --replication-factor "${REPLICATION_FACTOR}" \
        --config retention.ms="${retention}" \
        --config compression.type=lz4 \
        --config message.timestamp.type=CreateTime \
        2>&1 | tail -1

    echo -e "  ${GREEN}✓ ${topic}${RESET}"
done

echo ""
echo -e "${BLUE}Verifying topics...${RESET}"
docker exec "${KAFKA_CONTAINER}" kafka-topics \
    --bootstrap-server "${BOOTSTRAP_SERVERS}" \
    --list

echo ""
echo -e "${GREEN}All Kafka topics created successfully.${RESET}"
