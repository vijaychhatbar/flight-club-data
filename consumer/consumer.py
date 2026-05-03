#!/usr/bin/env python3
"""
Kafka to Iceberg Consumer
Reads flight data from Kafka (Avro) and writes to Apache Iceberg tables with DuckDB catalog.
Handles batching, deduplication, and error logging. Parquet files are written to local filesystem.
Uses a SQLite catalog for simplicity.
"""

import logging
import sys
import os
from datetime import datetime
from typing import List, Dict, Any, Optional
from pydantic import BaseModel
from prometheus_client import Counter, Gauge, start_http_server
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

# Prometheus metrics
MESSAGES_CONSUMED  = Counter("consumer_messages_consumed_total",  "Total Kafka messages consumed")
RECORDS_WRITTEN    = Counter("consumer_records_written_total",    "Total records written to Iceberg")
BATCHES_WRITTEN    = Counter("consumer_batches_written_total",    "Total batches written to Iceberg")
VALIDATION_ERRORS  = Counter("consumer_validation_errors_total",  "Pydantic FlightRecord validation failures")
ICEBERG_ERRORS     = Counter("consumer_iceberg_errors_total",     "Iceberg batch write failures")
BUFFER_SIZE        = Gauge("consumer_buffer_size",                "Current in-memory record buffer size")


class FlightRecord(BaseModel):
    icao24: str
    callsign: Optional[str] = None
    origin_country: Optional[str] = None
    time_position: Optional[int] = None
    last_contact: Optional[int] = None
    longitude: Optional[float] = None
    latitude: Optional[float] = None
    baro_altitude: Optional[float] = None
    on_ground: Optional[bool] = None
    velocity: Optional[float] = None
    true_track: Optional[float] = None
    vertical_rate: Optional[float] = None
    geo_altitude: Optional[float] = None
    squawk: Optional[str] = None
    spi: Optional[bool] = None
    position_source: Optional[int] = None
    fetch_timestamp: Optional[int] = None
    ingestion_time: str


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

        self.catalog = self._init_catalog()
        self.table = self._init_table()

        self.buffer: List[FlightRecord] = []
        self.last_write_time = time.time()

        self.stats = {"total_consumed": 0, "total_written": 0, "batches_written": 0, "errors": 0}

        start_http_server(8000)
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

        try:
            catalog.create_namespace(self.namespace)
            logger.info(f"Created namespace: {self.namespace}")
        except Exception:
            logger.debug(f"Namespace {self.namespace} already exists")

        return catalog

    def _init_table(self):
        """Initialize Iceberg table with schema"""
        table_identifier = f"{self.namespace}.{self.table_name}"

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
            NestedField(16, "position_source", LongType(), required=False),
            NestedField(17, "fetch_timestamp", LongType(), required=False),
            NestedField(18, "ingestion_time", TimestampType(), required=False),
        )

        try:
            table = self.catalog.load_table(table_identifier)
            logger.info(f"Loaded existing table: {table_identifier}")
        except Exception:
            logger.info(f"Creating new table: {table_identifier}")
            table = self.catalog.create_table(identifier=table_identifier, schema=schema)

        return table

    def convert_to_arrow(self, records: List[FlightRecord]) -> pa.Table:
        """Convert list of records to PyArrow table"""
        rows = []
        for record in records:
            row = record.model_dump()
            try:
                row["ingestion_time"] = datetime.fromisoformat(
                    row["ingestion_time"].replace("Z", "+00:00")
                )
            except Exception:
                logger.warning(f"Failed to parse ingestion_time: {row.get('ingestion_time')}")
                row["ingestion_time"] = datetime.utcnow()
            if row["position_source"] is not None:
                row["position_source"] = int(row["position_source"])
            rows.append(row)
        return pa.Table.from_pylist(rows)

    def write_batch(self):
        """Write buffered records to Iceberg"""
        if not self.buffer:
            return

        try:
            logger.info(f"Writing batch of {len(self.buffer)} records to Iceberg")

            seen = set()
            unique_records = []
            for record in self.buffer:
                key = (record.icao24, record.fetch_timestamp)
                if key not in seen:
                    seen.add(key)
                    unique_records.append(record)

            if len(unique_records) < len(self.buffer):
                logger.info(f"Removed {len(self.buffer) - len(unique_records)} duplicates")

            arrow_table = self.convert_to_arrow(unique_records)
            self.table.append(arrow_table)

            self.stats["total_written"] += len(unique_records)
            self.stats["batches_written"] += 1
            RECORDS_WRITTEN.inc(len(unique_records))
            BATCHES_WRITTEN.inc()

            logger.info(f"Successfully wrote {len(unique_records)} records to Iceberg")

            self.buffer.clear()
            BUFFER_SIZE.set(0)
            self.last_write_time = time.time()

        except Exception as e:
            logger.error(f"Error writing batch to Iceberg: {e}", exc_info=True)
            self.stats["errors"] += 1
            ICEBERG_ERRORS.inc()

    def should_write_batch(self) -> bool:
        """Check if we should write the current batch"""
        if len(self.buffer) >= self.batch_size:
            return True
        return (time.time() - self.last_write_time) >= self.batch_timeout

    def run(self):
        """Main consumer loop"""
        logger.info(f"Starting Iceberg consumer for topic: {self.topic}")
        logger.info(f"Batch size: {self.batch_size}, Timeout: {self.batch_timeout}s")
        logger.info(f"Writing to: {self.namespace}.{self.table_name}")

        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)

                if msg is None:
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
                    serialization_context = SerializationContext(self.topic, MessageField.VALUE)
                    raw = self.avro_deserializer(msg.value(), serialization_context)

                    if raw:
                        self.buffer.append(FlightRecord(**raw))
                        self.stats["total_consumed"] += 1
                        MESSAGES_CONSUMED.inc()
                        BUFFER_SIZE.set(len(self.buffer))

                        if self.stats["total_consumed"] % 1000 == 0:
                            logger.info(
                                f"Consumed {self.stats['total_consumed']} messages, "
                                f"buffer size: {len(self.buffer)}"
                            )

                        if self.should_write_batch():
                            self.write_batch()
                            logger.info(
                                f"Stats: Consumed={self.stats['total_consumed']}, "
                                f"Written={self.stats['total_written']}, "
                                f"Batches={self.stats['batches_written']}, "
                                f"Errors={self.stats['errors']}"
                            )

                except Exception as e:
                    logger.error(f"Error processing message: {e}", exc_info=True)
                    self.stats["errors"] += 1
                    VALIDATION_ERRORS.inc()

        except KeyboardInterrupt:
            logger.info("Received interrupt signal, shutting down...")
        except Exception as e:
            logger.error(f"Unexpected error: {e}", exc_info=True)
        finally:
            self.cleanup()

    def cleanup(self):
        """Clean up resources"""
        logger.info("Cleaning up...")
        if self.buffer:
            logger.info(f"Writing final batch of {len(self.buffer)} records")
            self.write_batch()
        self.consumer.close()
        logger.info(f"Final stats: {self.stats}")
        logger.info("Consumer shut down cleanly")


def main():
    try:
        consumer = IcebergConsumer()
        consumer.run()
    except Exception as e:
        logger.error(f"Failed to start consumer: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
