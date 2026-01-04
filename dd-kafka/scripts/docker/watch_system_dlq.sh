#!/usr/bin/env bash
# scripts/docker/watch_system_dlq.sh
# Watch SYSTEM DLQ (platform / infra failures)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/env.sh"

echo "KafkaDD :: WATCH (docker) | system-dlq=$SYSTEM_DLQ_TOPIC | bootstrap=$BOOTSTRAP_SERVERS"
echo "Ctrl+C to stop"
echo

exec docker exec -i "$KAFKA_CLI_SERVICE" \
  kafka-console-consumer \
    --bootstrap-server "$BOOTSTRAP_SERVERS" \
    --topic "$SYSTEM_DLQ_TOPIC" \
    --group "$CONSUMER_GROUP_ID" \
    --from-beginning
