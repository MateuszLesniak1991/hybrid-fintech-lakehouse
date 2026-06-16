# Local Setup

## 1. Requirements

Recommended environment:

- Ubuntu Server 24.04,
- Docker Engine,
- Docker Compose,
- Python 3.12,
- Python virtual environment,
- Git,
- at least 16 GB RAM,
- sufficient disk space.

---

## 2. Clone the repository

```bash
git clone git@github.com:MateuszLesniak1991/hybrid-fintech-lakehouse.git
cd hybrid-fintech-lakehouse
```

---

## 3. Create Python environment

```bash
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

---

## 4. Configure environment variables

```bash
cp .env.example .env
```

Required local variables:

```env
POSTGRES_DB=fintech
POSTGRES_USER=fintech_user
POSTGRES_PASSWORD=fintech_pass
POSTGRES_HOST=localhost
POSTGRES_PORT=5432

KAFKA_BOOTSTRAP_SERVERS=localhost:19092
KAFKA_TOPIC=transactions.raw

MINIO_ENDPOINT=http://localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_BUCKET=bronze
```

Azure secrets should be added only locally:

```env
AZURE_EVENTHUB_CONNECTION_STRING=...
AZURE_EVENTHUB_NAME=...
```

Never commit `.env`.

---

## 5. Start local services

```bash
docker compose up -d
docker compose ps
```

Expected services:

- PostgreSQL,
- pgAdmin,
- Redpanda,
- Redpanda Console,
- MinIO.

---

## 6. Local web interfaces

### pgAdmin

```text
http://SERVER_IP:5050
```

### Redpanda Console

```text
http://SERVER_IP:8080
```

### MinIO Console

```text
http://SERVER_IP:9001
```

---

## 7. Create source schema

```bash
docker compose exec -T postgres   psql -U fintech_user -d fintech   < sql/source/01_banking_source_schema.sql
```

Check:

```bash
docker compose exec postgres   psql -U fintech_user -d fintech   -c "\dt banking_source.*"
```

---

## 8. Generate historical banking data

Small validation run:

```bash
python generator/generate_historical_banking_postgres.py   --start-date 2026-05-16   --end-date 2026-06-15   --customers 100   --merchants 30   --atms 10   --transactions 1000   --batch-size 500   --reset
```

Full run:

```bash
python generator/generate_historical_banking_postgres.py   --start-date 2026-05-16   --end-date 2026-06-15   --customers 5000   --merchants 800   --atms 250   --transactions 250000   --batch-size 5000   --reset
```

---

## 9. Validate generated data

```bash
docker compose exec postgres   psql -U fintech_user -d fintech   -c "
SELECT
    MIN(event_time),
    MAX(event_time),
    COUNT(DISTINCT event_time::date),
    COUNT(*)
FROM banking_source.transactions;
"
```

Expected:

- 31 days,
- 250,000 transactions.

---

## 10. Create Redpanda topic

```bash
docker compose exec redpanda   rpk topic create banking.operational.events   --partitions 6   --replicas 1   --brokers localhost:9092
```

Check:

```bash
docker compose exec redpanda   rpk topic describe banking.operational.events   --brokers localhost:9092
```

---

## 11. Dry-run streaming replay

```bash
python streaming/replay_postgres_events_to_redpanda.py   --limit 5   --fresh-timestamps   --dry-run
```

---

## 12. Full streaming replay

```bash
python streaming/replay_postgres_events_to_redpanda.py   --topic banking.operational.events   --bootstrap-servers localhost:19092   --batch-size 1000   --sleep-ms 10   --fresh-timestamps   --mark-replayed
```

Monitor:

```bash
watch -n 5 'cd /home/dataeng/portfolio/hybrid-fintech-lakehouse && docker compose exec -T postgres psql -U fintech_user -d fintech -tAc "
SELECT
    COUNT(*) FILTER (WHERE replayed_flag = TRUE),
    COUNT(*) FILTER (WHERE replayed_flag = FALSE)
FROM banking_source.stream_events;
"'
```

---

## 13. Test hourly MinIO export

```bash
python batch/export_postgres_to_minio.py   --start 2026-05-16T01:00:00+00:00   --end 2026-05-16T01:59:59+00:00   --dataset transactions   --keep-local   --overwrite
```

---

## 14. Full MinIO export

```bash
python batch/export_postgres_to_minio.py   --start 2026-05-16T00:00:00+00:00   --end 2026-06-15T23:59:59+00:00   --dataset all   --overwrite
```

---

## 15. Stop environment

```bash
docker compose down
```

To keep local volumes, do not use `-v`.
