#!/usr/bin/env bash
# scripts/local/env.sh
# OTB local environment (Kafka on localhost)

set -euo pipefail

# Local Kafka
BOOTSTRAP_SERVERS="localhost:9092"

# Topic contract (LOCKED)
INPUT_TOPIC="kafkadd.input"
SYSTEM_DLQ_TOPIC="kafkadd.dlq.system"
BUSINESS_DLQ_TOPIC="kafkadd.dlq.business"

# Consumer group for CLI consumers
CONSUMER_GROUP_ID="kafkadd.otb"

export BOOTSTRAP_SERVERS
export INPUT_TOPIC SYSTEM_DLQ_TOPIC BUSINESS_DLQ_TOPIC
export CONSUMER_GROUP_ID

