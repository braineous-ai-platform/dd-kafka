#!/usr/bin/env bash
# scripts/docker/watch_input.sh
# Watch INPUT topic via docker (runs inside broker container)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/env.sh"

echo "KafkaDD :: WATCH (docker) | topic=$INPUT_TOPIC | bootstrap=$BOOTSTRAP_SERVERS | exec=$KAFKA_CLI_SERVICE"
echo "Ctrl+C to stop"
echo

# Prefer compose exec; fallback to docker exec by container name
CID="$($DOCKER_COMPOSE_BIN -f "$COMPOSE_FILE" ps -q "$KAFKA_CLI_SERVICE" 2>/dev/null || true)"
if [[ -n "${CID:-}" ]]; then
  exec $DOCKER_COMPOSE_BIN -f "$COMPOSE_FILE" exec -T "$KAFKA_CLI_SERVICE" \
    kafka-console-consumer \
      --bootstrap-server "$BOOTSTRAP_SERVERS" \
      --topic "$INPUT_TOPIC" \
      --group "$CONSUMER_GROUP_ID" \
      --from-beginning
fi

# fallback (if compose file mismatch but container_name exists)
exec docker exec -i "$KAFKA_CLI_SERVICE" \
  kafka-console-consumer \
    --bootstrap-server "$BOOTSTRAP_SERVERS" \
    --topic "$INPUT_TOPIC" \
    --group "$CONSUMER_GROUP_ID" \
    --from-beginning



