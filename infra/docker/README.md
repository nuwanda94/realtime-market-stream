# Local infrastructure

The Compose file lives at the repository root (`docker-compose.yml`) so a single
`docker compose up -d` from the project root starts the local stack.

## Services

| Service           | Host port | Purpose                                      |
|-------------------|-----------|----------------------------------------------|
| Redpanda          | 19092     | Kafka-compatible broker                      |
| Redpanda Console  | 8080      | Topic / consumer UI                          |
| Schema Registry   | 18081     | Redpanda schema registry                     |
| MinIO S3 API      | 9000      | Object store for Delta / Iceberg             |
| MinIO Console     | 9001      | Bucket UI                                    |
| Postgres          | 5432      | Airflow metadata (and future app state)      |
| Prometheus        | 9090      | Metrics scrape                               |
| Grafana           | 3000      | Dashboards (`admin` / `admin` by default)    |

## Start / stop

```bash
cp .env.example .env   # optional; Compose has local defaults
docker compose up -d
docker compose ps
docker compose down
```

The `minio-init` one-shot job creates the `market-lake` bucket if it is missing.
