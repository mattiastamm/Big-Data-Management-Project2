# Big Data Management — Project 2

Streaming pipeline using Kafka, Spark Structured Streaming, and Apache Iceberg with a medallion architecture (Bronze, Silver, Gold).

## Architecture

```
produce.py (local)        Docker containers
      |                  +--------------------------+
      |  localhost:9092  |                          |
      +----------------->|  Kafka                   |
                         |    |                     |
                         |    v                     |
                         |  Bronze (Spark)          |
                         |    Kafka -> Iceberg      |
                         |    |                     |
                         |    v                     |
                         |  Silver (Spark)          |
                         |    Clean + enrich        |
                         |    |                     |
                         |    v                     |
                         |  Gold (Spark)            |
                         |    Top 5 zones           |
                         |                          |
                         |  MinIO (S3 storage)      |
                         |  Zookeeper               |
                         +--------------------------+
```

### Containers

| Service | Purpose |
|---|---|
| **zookeeper** | Kafka coordination |
| **kafka** | Message broker, topic `taxi-trips` |
| **kafka-init** | Creates the `taxi-trips` topic on startup |
| **minio** | S3-compatible object storage for Iceberg tables |
| **minio-init** | Creates the `warehouse` bucket on startup |
| **bronze** | Spark Streaming: reads raw JSON from Kafka, writes as-is to `lakehouse.taxi.bronze` |
| **silver** | Spark Streaming: reads from bronze, cleans, deduplicates, enriches with zone names, writes to `lakehouse.taxi.silver` |
| **gold** | Spark Streaming: reads from silver, computes top 5 pickup zones by trip count, writes to `lakehouse.taxi.gold` |

### Scripts

| Script | Runs | Purpose |
|---|---|---|
| `src/produce.py` | Locally | Reads parquet files from `data/trips/` and publishes each row as a JSON message to Kafka |
| `src/streaming/bronze.py` | In Docker | Raw ingestion from Kafka to Iceberg |
| `src/streaming/silver.py` | In Docker | Cleaning, deduplication, enrichment |
| `src/streaming/gold.py` | In Docker | Aggregation (top 5 pickup zones) |

## Data

Place files in the following structure:

```
data/
  trips/           NYC yellow taxi parquet files
  lookup/          taxi_zone_lookup.parquet (zone enrichment)
```

## How to run

### 1. Start the pipeline

```bash
docker compose up --build -d
```

This starts Kafka, MinIO, and the three Spark streaming jobs (bronze, silver, gold). Wait ~30 seconds for all services to initialize.

### 2. Run the producer

```bash
python src/produce.py
```

This reads parquet files from `data/trips/` and sends each row to the `taxi-trips` Kafka topic. Progress is printed every 10,000 messages.

### 3. Verify the tables

Open a PySpark shell inside any of the Spark containers:

```bash
docker compose exec bronze pyspark
```

Then run:

```python
# Row counts
spark.sql("SELECT count(*) FROM lakehouse.taxi.bronze").show()
spark.sql("SELECT count(*) FROM lakehouse.taxi.silver").show()

# Sample rows
spark.sql("SELECT * FROM lakehouse.taxi.bronze LIMIT 5").show()
spark.sql("SELECT * FROM lakehouse.taxi.silver LIMIT 5").show()

# Gold table (top 5 pickup zones)
spark.sql("SELECT * FROM lakehouse.taxi.gold").show()
```

Silver should have fewer rows than bronze (cleaning removes ~4.5% of invalid rows).

### 4. Test restart (no duplicates)

The checkpoint mechanism ensures exactly-once processing. To verify:

```python
# Note the current count
spark.sql("SELECT count(*) FROM lakehouse.taxi.silver").show()
```

Then stop and restart the pipeline:

```bash
docker compose stop bronze silver gold
docker compose start bronze silver gold
```

Query the count again — it should be unchanged. No duplicates.

### 5. Shut down

```bash
docker compose down
```

This stops and removes all containers. The Iceberg table data persists in MinIO and checkpoints persist in `./checkpoints/`.

### 6. Clean up for a fresh run

Iceberg tables are stored in MinIO, not in the containers. To start completely fresh, drop the tables before shutting down:

```bash
docker compose exec bronze pyspark
```

```python
spark.sql("DROP TABLE IF EXISTS lakehouse.taxi.gold")
spark.sql("DROP TABLE IF EXISTS lakehouse.taxi.silver")
spark.sql("DROP TABLE IF EXISTS lakehouse.taxi.bronze")
```

Then:

```bash
rm -rf checkpoints
docker compose down
```

## Silver layer cleaning rules

| Rule | Description |
|---|---|
| Null timestamps | Rows missing pickup or dropoff datetime removed |
| Invalid timestamp order | Dropoff must be strictly after pickup |
| Trips > 24 hours | Unrealistic for standard taxi trips |
| Non-positive distance | Completed trips must have positive distance |
| Negative fares | Negative fares are invalid; zero fares permitted |
| Negative total amounts | Negative totals are invalid; zero permitted |
| Negative passenger counts | Only clearly invalid negatives removed |
| Invalid location IDs | PULocationID and DOLocationID must be positive |
| Deduplication | 6-column composite key (pickup/dropoff time, locations, distance, fare) |

## Silver layer enrichment

- **trip_duration_minutes** — derived from pickup and dropoff timestamps
- **pickup_date** — date extracted from pickup timestamp
- **pickup_zone / dropoff_zone** — joined from taxi zone lookup table
- **ingested_at** — timestamp of when the row was processed into silver

## Medallion schema overview

| Column | Bronze | Silver | Gold |
|---|---|---|---|
| tpep_pickup_datetime | BIGINT (Unix ms) | TIMESTAMP | — |
| tpep_dropoff_datetime | BIGINT (Unix ms) | TIMESTAMP | — |
| passenger_count | DOUBLE | INT | — |
| PULocationID / DOLocationID | BIGINT | INT | — |
| fare_amount / total_amount | DOUBLE | DOUBLE | — |
| pickup_zone / dropoff_zone | — | STRING (enriched) | STRING |
| trip_duration_minutes | — | DOUBLE (derived) | — |
| pickup_date | — | DATE (derived) | — |
| ingested_at | — | TIMESTAMP (audit) | — |
| trip_count | — | — | BIGINT |

Bronze preserves the raw Kafka payload unchanged. Silver recasts types, applies cleaning rules, deduplicates, and enriches with zone names. Gold collapses everything into a 2-column top-5 aggregation.

## Gold partitioning strategy

The gold table is `PARTITIONED BY (pickup_zone)`. Each of the 5 zones is stored in its own Iceberg data file, so a query filtering on one zone reads a single partition. Iceberg also records a new snapshot on every micro-batch recompute, preserving full history.

## Iceberg snapshot history

```python
spark.sql("""
    SELECT snapshot_id, committed_at, operation, summary
    FROM lakehouse.taxi.gold.snapshots
    ORDER BY committed_at
""").show(truncate=False)
```

```
+-------------------+-----------------------+---------+
|snapshot_id        |committed_at           |operation|
+-------------------+-----------------------+---------+
|1054311666291042732|2026-04-05 10:52:38.844|overwrite|
+-------------------+-----------------------+---------+
added-data-files=1, added-records=5, total-records=5
```

Time travel:
```python
spark.read.option("snapshot-id", "1054311666291042732").table("lakehouse.taxi.gold").show()
```

## Custom scenario — Top 5 pickup zones

Silver enriches each trip with `pickup_zone` via a broadcast join on `taxi_zone_lookup.parquet`. Gold aggregates and keeps the top 5:

```sql
SELECT pickup_zone, count(*) AS trip_count
FROM lakehouse.taxi.silver
GROUP BY pickup_zone
ORDER BY trip_count DESC
LIMIT 5
```

| Rank | pickup_zone | trip_count |
|------|-------------|------------|
| 1 | Upper East Side South | 1905 |
| 2 | Upper East Side North | 1852 |
| 3 | Penn Station/Madison Sq West | 1331 |
| 4 | JFK Airport | 1316 |
| 5 | Upper West Side South | 1232 |
