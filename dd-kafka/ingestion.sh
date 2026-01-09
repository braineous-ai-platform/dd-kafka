curl -v http://localhost:8080/api/ingestion \
  -H "Content-Type: application/json" \
  -H "Accept: application/json" \
  --data-binary '{
    "payload":"{\"kafka\":{\"topic\":\"ingestion\",\"partition\":0,\"offset\":1,\"timestamp\":1700000000000},\"payload\":{\"encoding\":\"base64\",\"value\":\"SGVsbG8=\"}}"
  }'
