#!/usr/bin/env bash
set -euo pipefail

BASE_URL="http://localhost:8080"
ENDPOINT="/api/ingestion"

REQ1='{
  "stream":"ingestion",
  "payload":"{\"kafka\":{\"topic\":\"requests\",\"partition\":3,\"offset\":48192,\"timestamp\":1767114000123},\"payload\":{\"encoding\":\"base64\",\"value\":\"AAECAwQFBgcICQ==\"}}"
}'

REQ2='{
  "stream":"ingestion",
  "payload":"{\"kafka\":{\"topic\":\"requests\",\"partition\":3,\"offset\":48192,\"timestamp\":1767114000123},\"payload\":{\"encoding\":\"base64\",\"value\":\"AAECAwQFBgcICg==\"}}"
}'

echo "===== REQ1 x2 ====="
for i in 1 2; do
  echo ""
  echo "---- REQ1 CALL #$i ----"
  curl -i -X POST "$BASE_URL$ENDPOINT" \
    -H "Content-Type: application/json" \
    -H "Accept: application/json" \
    --data-binary "$REQ1"
done

echo ""
echo "===== REQ2 x1 ====="
curl -i -X POST "$BASE_URL$ENDPOINT" \
  -H "Content-Type: application/json" \
  -H "Accept: application/json" \
  --data-binary "$REQ2"









