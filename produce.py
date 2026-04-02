"""
produce.py — Reads NYC yellow taxi parquet files from data/ and publishes
each row as a JSON message to the Kafka topic 'taxi-trips'.
"""

import json
import time
import glob
import sys

import pandas as pd
from kafka import KafkaProducer

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "taxi-trips"
SLEEP_BETWEEN_MSGS = 0.02   # seconds — simulates a live stream
PROGRESS_EVERY = 500


def main():
    parquet_files = sorted(glob.glob("data/*.parquet"))
    if not parquet_files:
        print("No parquet files found in data/. Exiting.")
        sys.exit(1)

    print(f"Found {len(parquet_files)} parquet file(s): {parquet_files}")

    print(f"Connecting to Kafka at {KAFKA_BOOTSTRAP} ...")
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    print("Connected.")

    total_sent = 0

    for filepath in parquet_files:
        print(f"\nReading {filepath} ...")
        df = pd.read_parquet(filepath)

        # Convert datetime columns to unix milliseconds so JSON is serialisable
        for col in df.select_dtypes(include=["datetime64[ns]", "datetime64[us]"]).columns:
            df[col] = df[col].astype("int64") // 1_000_000  # ns → ms

        print(f"  Rows in file: {len(df):,}")

        for _, row in df.iterrows():
            producer.send(TOPIC, value=row.to_dict())
            total_sent += 1

            if total_sent % PROGRESS_EVERY == 0:
                print(f"  [{filepath}] Messages sent: {total_sent:,}")

            time.sleep(SLEEP_BETWEEN_MSGS)

    producer.flush()
    print(f"\nDone. Total messages sent: {total_sent:,}")


if __name__ == "__main__":
    main()
