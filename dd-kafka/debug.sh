curl -X POST http://localhost:8080/api/ingestion \
  -H "Content-Type: application/json" \
  -H "Accept: application/json" \
  -d '{
        "msg": "Hello KafkaDD 👋"
      }'
