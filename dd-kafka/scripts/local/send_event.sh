#!/usr/bin/env bash
# scripts/local/send_event.sh
# Send a real client event to local app (port 8080)

set -euo pipefail

APP_HOST="localhost"
APP_PORT="8080"
ENDPOINT="/simulate"

URL="http://${APP_HOST}:${APP_PORT}${ENDPOINT}"

echo "KafkaDD :: SEND EVENT (local)"
echo "POST $URL"
echo

PAYLOAD='{
  "payload": "{ \"name\": \"test\", \"email\": \"test@test.com\" }"
}'

OUT_FILE="$(mktemp -t kafkadd_send_event.XXXXXX)"

# curl:
# -sS: silent but show errors
# -o: write body to file
# -w: print http code only
# --connect-timeout: fast fail if service down
HTTP_CODE="$(curl -sS -o "$OUT_FILE" -w "%{http_code}" \
  --connect-timeout 2 \
  -X POST "$URL" \
  -H "Content-Type: application/json" \
  -d "$PAYLOAD")"

# If service is down, curl exits non-zero and script stops (set -e).
# So if we’re here, we got a real HTTP code.
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
