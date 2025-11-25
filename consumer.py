#!/usr/bin/env python3
"""
Kafka to Iceberg Consumer
Reads flight data from Kafka (Avro) and writes to Apache Iceberg tables with DuckDB catalog.
Handles batching, deduplication, and error logging. Parquet files are written to local filesystem.
I use a SQLite catalog for simplicity. This can be changed to other catalog types as needed.

My system has a /mnt/nvme mounted volume for Iceberg data storage.
"""

import logging
import sys
import os
from datetime import datetime
from typing import List, Dict, Any
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField,
    StringType,
    DoubleType,
    LongType,
    BooleanType,
    TimestampType,
)
from dotenv import load_dotenv
import time

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class IcebergConsumer:
    """Consumes flight data from Kafka and writes to Iceberg"""

    def __init__(self):
        # Kafka configuration
        bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.topic = os.getenv("KAFKA_TOPIC", "flight-data")
        self.group_id = os.getenv("CONSUMER_GROUP_ID", "iceberg-consumer-group")

        # Batch configuration
        self.batch_size = int(os.getenv("BATCH_SIZE", "1000"))
        self.batch_timeout = int(os.getenv("BATCH_TIMEOUT_SECONDS", "300"))

        # Iceberg configuration
        self.catalog_path = os.getenv("ICEBERG_CATALOG_PATH", "/data/iceberg-warehouse")
        self.namespace = os.getenv("ICEBERG_NAMESPACE", "aviation")
        self.table_name = os.getenv("ICEBERG_TABLE", "flight_data")

        # Schema Registry configuration
        schema_registry_url = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")

        logger.info(f"Initializing Schema Registry client: {schema_registry_url}")
        schema_registry_conf = {"url": schema_registry_url}
        schema_registry_client = SchemaRegistryClient(schema_registry_conf)

        # Create Avro deserializer
        self.avro_deserializer = AvroDeserializer(
            schema_registry_client, from_dict=lambda data, ctx: data
        )

        logger.info(f"Initializing Kafka consumer for {bootstrap_servers}")
        consumer_conf = {
            "bootstrap.servers": bootstrap_servers,
            "group.id": self.group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
            "auto.commit.interval.ms": 5000,
        }

        self.consumer = Consumer(consumer_conf)
        self.consumer.subscribe([self.topic])

        # Initialize Iceberg catalog and table
        self.catalog = self._init_catalog()
        self.table = self._init_table()

        # Buffer for batching
        self.buffer: List[Dict[str, Any]] = []
        self.last_write_time = time.time()

        self.stats = {"total_consumed": 0, "total_written": 0, "batches_written": 0, "errors": 0}

        logger.info("Iceberg Consumer initialized successfully")

    def _init_catalog(self):
        """Initialize Iceberg catalog with SQLite"""
        catalog_config = {
            "type": "sql",
            "uri": f"sqlite:///{self.catalog_path}/catalog.db",
            "warehouse": self.catalog_path,
        }

        logger.info(f"Initializing Iceberg catalog at {self.catalog_path}")
        os.makedirs(self.catalog_path, exist_ok=True)

        catalog = load_catalog("local", **catalog_config)

        # Create namespace if it doesn't exist
        try:
            catalog.create_namespace(self.namespace)
            logger.info(f"Created namespace: {self.namespace}")
        except Exception:
            logger.debug(f"Namespace {self.namespace} already exists")

        return catalog

    def _init_table(self):
        """Initialize Iceberg table with schema"""
        table_identifier = f"{self.namespace}.{self.table_name}"

        # Define schema matching the Avro schema - all optional except timestamp
        schema = Schema(
            NestedField(1, "icao24", StringType(), required=False),
            NestedField(2, "callsign", StringType(), required=False),
            NestedField(3, "origin_country", StringType(), required=False),
            NestedField(4, "time_position", LongType(), required=False),
            NestedField(5, "last_contact", LongType(), required=False),
            NestedField(6, "longitude", DoubleType(), required=False),
            NestedField(7, "latitude", DoubleType(), required=False),
            NestedField(8, "baro_altitude", DoubleType(), required=False),
            NestedField(9, "on_ground", BooleanType(), required=False),
            NestedField(10, "velocity", DoubleType(), required=False),
            NestedField(11, "true_track", DoubleType(), required=False),
            NestedField(12, "vertical_rate", DoubleType(), required=False),
            NestedField(13, "geo_altitude", DoubleType(), required=False),
            NestedField(14, "squawk", StringType(), required=False),
            NestedField(15, "spi", BooleanType(), required=False),
            NestedField(16, "position_source", LongType(), required=False),  # Changed to Long
            NestedField(17, "fetch_timestamp", LongType(), required=False),
            NestedField(
                18, "ingestion_time", TimestampType(), required=False
            ),  # Changed to optional
        )

        # Create unpartitioned table (partitioning not supported with append())
        try:
            # Try to load existing table
            table = self.catalog.load_table(table_identifier)
            logger.info(f"Loaded existing table: {table_identifier}")
        except Exception:
            # Create new table without partitioning
            logger.info(f"Creating new unpartitioned table: {table_identifier}")
            table = self.catalog.create_table(identifier=table_identifier, schema=schema)
            logger.info(f"Created table: {table_identifier}")

        return table

    def convert_to_arrow(self, records: List[Dict[str, Any]]) -> pa.Table:
        """Convert list of records to PyArrow table"""
        # Parse ISO timestamps and ensure types match
        for record in records:
            # Convert ingestion_time to datetime
            if "ingestion_time" in record and isinstance(record["ingestion_time"], str):
                try:
                    record["ingestion_time"] = datetime.fromisoformat(
                        record["ingestion_time"].replace("Z", "+00:00")
                    )
                except Exception:
                    logger.warning(f"Failed to parse ingestion_time: {record.get('ingestion_time')}")
                    record["ingestion_time"] = datetime.utcnow()

            # Ensure position_source is int/long if present
            if "position_source" in record and record["position_source"] is not None:
                record["position_source"] = int(record["position_source"])

        # Convert to PyArrow table
        return pa.Table.from_pylist(records)

    def write_batch(self):
        """Write buffered records to Iceberg"""
        if not self.buffer:
            return

        try:
            logger.info(f"Writing batch of {len(self.buffer)} records to Iceberg")

            # Remove duplicates based on icao24 and fetch_timestamp
            seen = set()
            unique_records = []
            for record in self.buffer:
                key = (record.get("icao24"), record.get("fetch_timestamp"))
                if key not in seen:
                    seen.add(key)
                    unique_records.append(record)

            if len(unique_records) < len(self.buffer):
                logger.info(f"Removed {len(self.buffer) - len(unique_records)} duplicates")

            # Convert to Arrow table
            arrow_table = self.convert_to_arrow(unique_records)

            # Append to Iceberg table
            self.table.append(arrow_table)

            self.stats["total_written"] += len(unique_records)
            self.stats["batches_written"] += 1

            logger.info(f"Successfully wrote {len(unique_records)} records to Iceberg")

            # Clear buffer
            self.buffer.clear()
            self.last_write_time = time.time()

        except Exception as e:
            logger.error(f"Error writing batch to Iceberg: {e}", exc_info=True)
            self.stats["errors"] += 1
            # Don't clear buffer on error, will retry next time

    def should_write_batch(self) -> bool:
        """Check if we should write the current batch"""
        if len(self.buffer) >= self.batch_size:
            logger.debug(f"Batch size reached: {len(self.buffer)}")
            return True

        time_elapsed = time.time() - self.last_write_time
        if time_elapsed >= self.batch_timeout:
            logger.debug(f"Batch timeout reached: {time_elapsed:.2f}s")
            return True

        return False

    def run(self):
        """Main consumer loop"""
        logger.info(f"Starting Iceberg consumer for topic: {self.topic}")
        logger.info(f"Batch size: {self.batch_size}, Timeout: {self.batch_timeout}s")
        logger.info(f"Writing to: {self.namespace}.{self.table_name}")

        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)

                if msg is None:
                    # No message, check if we should write buffered data
                    if self.buffer and self.should_write_batch():
                        self.write_batch()
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.debug(f"Reached end of partition {msg.partition()}")
                    else:
                        logger.error(f"Consumer error: {msg.error()}")
                        self.stats["errors"] += 1
                    continue

                try:
                    # Deserialize Avro message
                    serialization_context = SerializationContext(self.topic, MessageField.VALUE)
                    flight_data = self.avro_deserializer(msg.value(), serialization_context)

                    if flight_data:
                        self.buffer.append(flight_data)
                        self.stats["total_consumed"] += 1

                        # Log progress
                        if self.stats["total_consumed"] % 1000 == 0:
                            logger.info(
                                f"Consumed {self.stats['total_consumed']} messages, "
                                f"buffer size: {len(self.buffer)}"
                            )

                        # Check if we should write batch
                        if self.should_write_batch():
                            self.write_batch()

                            # Print stats
                            logger.info(
                                f"Stats: Consumed={self.stats['total_consumed']}, "
                                f"Written={self.stats['total_written']}, "
                                f"Batches={self.stats['batches_written']}, "
                                f"Errors={self.stats['errors']}"
                            )

                except Exception as e:
                    logger.error(f"Error processing message: {e}", exc_info=True)
                    self.stats["errors"] += 1

        except KeyboardInterrupt:
            logger.info("Received interrupt signal, shutting down...")
        except Exception as e:
            logger.error(f"Unexpected error: {e}", exc_info=True)
        finally:
            self.cleanup()

    def cleanup(self):
        """Clean up resources"""
        logger.info("Cleaning up...")

        # Write any remaining buffered data
        if self.buffer:
            logger.info(f"Writing final batch of {len(self.buffer)} records")
            self.write_batch()

        # Close consumer
        self.consumer.close()

        logger.info(f"Final stats: {self.stats}")
        logger.info("Consumer shut down cleanly")


def main():
    """Main entry point"""
    try:
        consumer = IcebergConsumer()
        consumer.run()
    except Exception as e:
        logger.error(f"Failed to start consumer: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
