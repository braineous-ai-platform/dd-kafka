#!/usr/bin/env bash
# scripts/docker/send_event.sh
# Send a real client event to Docker app (port 80)

set -euo pipefail

APP_HOST="localhost"
APP_PORT="80"
ENDPOINT="/simulate"

URL="http://${APP_HOST}:${APP_PORT}${ENDPOINT}"

echo "KafkaDD :: SEND EVENT (docker)"
echo "POST $URL"
echo

PAYLOAD='{
  "payload": "{ \"name\": \"test\", \"email\": \"test@test.com\" }"
}'

OUT_FILE="$(mktemp -t kafkadd_send_event.XXXXXX)"

HTTP_CODE="$(curl -sS -o "$OUT_FILE" -w "%{http_code}" \
  --connect-timeout 2 \
  -X POST "$URL" \
  -H "Content-Type: application/json" \
  -d "$PAYLOAD")"

if [[ "$HTTP_CODE" != 2* ]]; then
  echo "❌ Request failed (HTTP $HTTP_CODE)"
  echo "Response:"
  cat "$OUT_FILE"
  rm -f "$OUT_FILE"
  exit 1
fi

echo "✔ Event accepted (HTTP $HTTP_CODE)"
cat "$OUT_FILE"
rm -f "$OUT_FILE"
echo

