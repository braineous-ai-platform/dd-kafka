# Docker Setup

KafkaDD provides a Docker Compose setup for running the full stack locally.

This document explains:
- what runs in Docker
- how services communicate
- how configuration is supplied (file-based)

---

## Prerequisites

- Docker
- Docker Compose

---

## Files and layout

KafkaDD expects these files at the **same level**:

- `docker-compose.yml`
- `dd-default-docker.properties`

Docker Compose is responsible for mounting or exposing the configuration file.
KafkaDD reads configuration using `DD_CONFIG_PATH`.

KafkaDD does **not** read Docker Compose files directly.

---

## Start the stack

From the directory containing `docker-compose.yml`:

```bash
docker compose up
```

The ingestion service is exposed to the host at:

* POST http://localhost:8080/api/ingestion

Use [GETTING_STARTED.md](GETTING_STARTED.md) for a verified request payload.

### Service addressing (Docker DNS)

Inside Docker:

* services communicate using Docker DNS service names

* localhost is not used between containers

Example (internal calls):

* Ingestion → Producer: http://dd-module-kafka-producer:8081

* Ingestion → DLQ service: http://dd-module-dlq-service:8083

If you see localhost in internal service URLs while running under Docker,
dd.env is likely incorrect.

### Configuration in Docker

KafkaDD configuration is file-based.

Docker Compose provides the path via DD_CONFIG_PATH, typically pointing at:

* dd-default-docker.properties

In Docker mode, the configuration file should set:

```arduino
dd.env=docker
```

### Rebuild when code changes

If behavior does not match your latest code or configuration, rebuild to avoid stale images:

```
docker compose down -v
docker compose build --no-cache
docker compose up
```

### Common pitfalls
- **Internal calls using `localhost`**
    - Ensure `dd.env=docker` in `dd-default-docker.properties`


- ** Config file not found
  - Ensure dd-default-docker.properties exists alongside docker-compose.yml
  - Ensure DD_CONFIG_PATH points to the mounted file

- **Stale containers/images
  - Rebuild with --no-cache and restart

  
### Next steps

* See [GETTING_STARTED.md](GETTING_STARTED.md) for a runnable ingestion example

* See [CONFIGURATION.md](CONFIGURATION.md) for file-based configuration details

* See [OPERATIONS.md](OPERATIONS.md) for DLQ inspection and replay semantics


