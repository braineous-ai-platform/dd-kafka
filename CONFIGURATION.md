# Configuration

KafkaDD uses **explicit, file-based configuration only**.

There is no automatic configuration discovery and no implicit environment-based configuration.

All runtime configuration is loaded from a properties file.

---

## Configuration model

KafkaDD reads configuration from **exactly one source**:

- a properties file provided explicitly via `DD_CONFIG_PATH`

If no configuration file is provided, KafkaDD falls back to built-in defaults.

No other configuration sources are used.

---

## Configuration files

KafkaDD supports two standard configuration files:

- `dd-default.properties`  
  Used for **local development**

- `dd-default-docker.properties`  
  Used when running under **Docker Compose**

These files are regular Java properties files.

---

## Local configuration

For local development, provide the configuration file path explicitly:

```bash
export DD_CONFIG_PATH=./config/dd-default.properties
```

A common and recommended layout is:

```arduino
dd-pack/
  config/
    dd-default.properties
```
Using a relative path keeps configuration versioned, visible, and reproducible.

### Docker Compose configuration

When running under Docker Compose:

* dd-default-docker.properties must exist at the same level as docker-compose.yml

* Docker Compose mounts or references this file explicitly

* KafkaDD loads this file via DD_CONFIG_PATH

KafkaDD does not read Docker Compose files directly.

Docker is responsible only for:

* process execution

* networking

* file mounting

### Configuration path (DD_CONFIG_PATH)

DD_CONFIG_PATH must point to a readable properties file.

Examples:

```bash
export DD_CONFIG_PATH=./config/dd-default.properties
```
```
export DD_CONFIG_PATH=/app/config/dd-default-docker.properties
```

KafkaDD does not:

* search directories

* infer file names

* guess locations

If the file is not found, KafkaDD will start using built-in defaults.

### ⚠️ Important warning

Do NOT place configuration files at the root of your filesystem (/).

Avoid paths such as:

```arduino
/dd-default.properties
```

Instead:

* keep configuration inside the project directory, or

* choose a location explicitly accessible to the running service

* Recommended patterns:

```arduino
./config/dd-default.properties
```

```arduino
dd-pack/config/dd-default.properties
```
This avoids permission issues, accidental overrides, and deployment surprises.

### Common configuration properties

| Property                    | Description                     | Example                   |
| --------------------------- | ------------------------------- | ------------------------- |
| `dd.env`                    | Runtime environment             | `local`, `docker`, `test` |
| `dd.http.port`              | HTTP port for ingestion service | `8080`                    |
| `dd.feature.replay.enabled` | Enable replay features          | `true`                    |

### Runtime environment (dd.env)

The dd.env property controls internal service resolution.

dd.env=local

* Used for local JVM execution

* Internal services resolve to localhost

dd.env=docker

* Used when running under Docker Compose

* nInternal services resolve using Docker DNS service names

* localhost is not used between containers

### Verification

To verify configuration is loaded correctly:

* Check startup logs for the resolved config path

* Confirm dd.env matches the execution mode

* Confirm internal service URLs match expectations

If behavior does not match configuration:

* confirm DD_CONFIG_PATH is correct

* confirm the file is readable

* rebuild Docker images if necessary

### Next steps

* See [GETTING_STARTED.md](GETTING_STARTED.md).md for a runnable example

* See [DOCKER.md](DOCKER.md) for Docker topology and networking

* See [OPERATIONS.md](OPERATIONS.md) for DLQ inspection and replay semantics









