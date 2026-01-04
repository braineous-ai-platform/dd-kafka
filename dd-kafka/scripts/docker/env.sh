#!/usr/bin/env bash
# scripts/docker/env.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

DOCKER_COMPOSE_BIN="${DOCKER_COMPOSE_BIN:-docker compose}"
COMPOSE_FILE="${COMPOSE_FILE:-$PROJECT_ROOT/docker-compose.yml}"

# IMPORTANT: run kafka-console-consumer from broker container/service
KAFKA_CLI_SERVICE="${KAFKA_CLI_SERVICE:-broker}"

BOOTSTRAP_SERVERS="broker:29092"

INPUT_TOPIC="kafkadd.input"
SYSTEM_DLQ_TOPIC="kafkadd.dlq.system"
BUSINESS_DLQ_TOPIC="kafkadd.dlq.business"
CONSUMER_GROUP_ID="kafkadd.otb"

export PROJECT_ROOT DOCKER_COMPOSE_BIN COMPOSE_FILE
export KAFKA_CLI_SERVICE
export BOOTSTRAP_SERVERS
export INPUT_TOPIC SYSTEM_DLQ_TOPIC BUSINESS_DLQ_TOPIC
export CONSUMER_GROUP_ID





