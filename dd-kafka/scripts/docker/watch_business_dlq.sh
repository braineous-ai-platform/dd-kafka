#!/usr/bin/env bash
# scripts/docker/watch_business_dlq.sh
# Watch BUSINESS DLQ (domain / policy rejections)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/env.sh"

echo "KafkaDD :: WATCH (docker) | business-dlq=$BUSINESS_DLQ_TOPIC | bootstrap=$BOOTSTRAP_SERVERS"
echo "Ctrl+C to stop"
echo

exec docker exec -i "$KAFKA_CLI_SERVICE" \
  kafka-console-consumer \
    --bootstrap-server "$BOOTSTRAP_SERVERS" \
    --topic "$BUSINESS_DLQ_TOPIC" \
    --group "$CONSUMER_GROUP_ID" \
    --from-beginning
