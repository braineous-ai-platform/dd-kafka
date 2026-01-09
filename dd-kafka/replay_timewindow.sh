curl -v -X POST http://localhost:8080/api/replay/time-window \
  -H "Content-Type: application/json" \
  -H "Accept: application/json" \
  -d '{
    "stream": "ingestion",
    "reason": "manual-curl",
    "fromTime": "2026-01-09T00:00:00Z",
    "toTime":   "2026-01-10T00:00:00Z"
  }'
