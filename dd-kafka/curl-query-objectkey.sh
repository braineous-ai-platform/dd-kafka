BASE=http://localhost:8080

curl -v -i -X POST "$BASE/api/replay/time-object-key" \
  -H "Content-Type: application/json" \
  -d '
    {
        "stream": "ingestion",
        "reason": "curl-test",
        "objectKey": ""DD-ING-20260111-299801806076916",
        "fromTime": "2026-01-07T00:00:00Z",
        "toTime":   "2026-01-07T01:00:00Z"
      }
  '
