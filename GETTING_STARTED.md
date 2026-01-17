# Getting Started

This guide gets KafkaDD running locally using Docker Compose and verifies ingestion with a real payload.

The goal is simple:

**clone → run → send one request → receive an ingestionId**

---

## Prerequisites

- Docker
- Docker Compose
- Java
- Maven

---

## Quick Start (Docker)

### 1. Clone the repository

```bash
git clone https://github.com/braineous-ai-platform/dd-kafka.git
cd dd-kafka
cd dd-kafka
````

### 2. Build the project
```bash
mvn clean package
```

### 3. Start the stack
```bash
docker compose up
```
Leave this running in a terminal.

The ingestion service will be available at:

```bash
POST http://localhost:8080/api/ingestion
```

### Verify ingestion (single request)
In a new terminal, run:
```bash
curl -i -X POST "http://localhost:8080/api/ingestion" \
  -H "Content-Type: application/json" \
  -H "Accept: application/json" \
  --data-binary '{
    "stream": "ingestion",
    "payload": "{\"kafka\":{\"topic\":\"requests\",\"partition\":3,\"offset\":48192,\"timestamp\":1767114000123},\"payload\":{\"encoding\":\"base64\",\"value\":\"AAECAwQFBgcICQ==\"}}"
  }'
```

### Expected result

* HTTP 200 response

* Response body contains an ingestionId

### Determinism check (optional)

Create the script

Save the following as ingestion.sh:

```bash
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
```

### Run the script

```bash
chmod +x ingestion.sh
./ingestion.sh
```

### Expected behavior

* Both REQ1 calls return the same ingestionId

* REQ2 returns a different ingestionId

## Next steps

- Read [README.md](README.md) for the conceptual model
- See [DOCKER.md](DOCKER.md) for Docker topology and environment behavior
- See [CONFIGURATION.md](CONFIGURATION.md) for configuration precedence
- See [OPERATIONS.md](OPERATIONS.md) for DLQ inspection and replay semantics






