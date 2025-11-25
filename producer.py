#!/usr/bin/env python3
"""
OpenSky API to Kafka Producer with Schema Registry
Fetches flight data from OpenSky Network API and streams to Kafka using Avro serialization
Uses OAuth2 token authentication
"""

import time
import logging
import sys
from datetime import datetime
from typing import Optional, Dict, Any
import requests
from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Avro schema for flight data
FLIGHT_SCHEMA = """
{
    "type": "record",
    "name": "FlightData",
    "namespace": "com.aviation.opensky",
    "fields": [
        {"name": "icao24", "type": "string"},
        {"name": "callsign", "type": ["null", "string"], "default": null},
        {"name": "origin_country", "type": ["null", "string"], "default": null},
        {"name": "time_position", "type": ["null", "long"], "default": null},
        {"name": "last_contact", "type": ["null", "long"], "default": null},
        {"name": "longitude", "type": ["null", "double"], "default": null},
        {"name": "latitude", "type": ["null", "double"], "default": null},
        {"name": "baro_altitude", "type": ["null", "double"], "default": null},
        {"name": "on_ground", "type": ["null", "boolean"], "default": null},
        {"name": "velocity", "type": ["null", "double"], "default": null},
        {"name": "true_track", "type": ["null", "double"], "default": null},
        {"name": "vertical_rate", "type": ["null", "double"], "default": null},
        {"name": "geo_altitude", "type": ["null", "double"], "default": null},
        {"name": "squawk", "type": ["null", "string"], "default": null},
        {"name": "spi", "type": ["null", "boolean"], "default": null},
        {"name": "position_source", "type": ["null", "int"], "default": null},
        {"name": "fetch_timestamp", "type": ["null", "long"], "default": null},
        {"name": "ingestion_time", "type": "string"}
    ]
}
"""


class OpenSkyProducer:
    """Fetches flight data from OpenSky API and publishes to Kafka with Schema Registry"""

    def __init__(self):
        # OpenSky API configuration
        self.api_url = os.getenv("OPENSKY_API_URL", "https://opensky-network.org/api/states/all")
        self.client_id = os.getenv("OPENSKY_CLIENT_ID", "")
        self.client_secret = os.getenv("OPENSKY_CLIENT_SECRET", "")
        self.auth_url = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
        self.fetch_interval = int(os.getenv("FETCH_INTERVAL_SECONDS", "300"))

        # OAuth token
        self.access_token = None
        self.token_expiry = 0

        # Get initial token if credentials provided
        if self.client_id and self.client_secret:
            logger.info("Using OAuth2 token authentication")
            self.refresh_token()
        else:
            logger.info("No credentials provided, using anonymous access")

        # Kafka configuration
        bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.topic = os.getenv("KAFKA_TOPIC", "flight-data")

        # Schema Registry configuration
        schema_registry_url = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")

        logger.info(f"Initializing Schema Registry client: {schema_registry_url}")
        schema_registry_conf = {"url": schema_registry_url}
        schema_registry_client = SchemaRegistryClient(schema_registry_conf)

        # Create Avro serializer
        self.avro_serializer = AvroSerializer(
            schema_registry_client, FLIGHT_SCHEMA, lambda flight, ctx: flight
        )

        # Kafka Producer configuration
        logger.info(f"Initializing Kafka producer for {bootstrap_servers}")
        producer_conf = {
            "bootstrap.servers": bootstrap_servers,
            "acks": "all",
            "compression.type": "snappy",
            "retries": 3,
            "client.id": "opensky-producer",
        }
        self.producer = Producer(producer_conf)

        self.stats = {"total_fetches": 0, "total_flights": 0, "total_sent": 0, "errors": 0}

        logger.info("OpenSky Producer initialized successfully with Schema Registry")

    def refresh_token(self):
        """Get a new OAuth2 access token"""
        try:
            logger.info("Requesting new OAuth2 access token...")

            data = {
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            }

            response = requests.post(
                self.auth_url,
                data=data,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                timeout=10,
            )
            response.raise_for_status()

            token_data = response.json()
            self.access_token = token_data["access_token"]
            # Usually token expires in 300 seconds, refresh before that
            expires_in = token_data.get("expires_in", 300)
            self.token_expiry = time.time() + expires_in - 30  # Refresh 30s early

            logger.info(f"OAuth2 token obtained successfully (expires in {expires_in}s)")

        except Exception as e:
            logger.error(f"Failed to get OAuth2 token: {e}")
            self.access_token = None
            self.token_expiry = 0

    def is_token_valid(self) -> bool:
        """Check if current token is still valid"""
        return self.access_token and time.time() < self.token_expiry

    def fetch_flight_data(self) -> Optional[Dict[str, Any]]:
        """Fetch current flight data from OpenSky API"""
        try:
            # Refresh token if needed
            if self.client_id and self.client_secret and not self.is_token_valid():
                self.refresh_token()

            # Prepare headers
            headers = {}
            if self.access_token:
                headers["Authorization"] = f"Bearer {self.access_token}"

            response = requests.get(self.api_url, headers=headers, timeout=30)
            response.raise_for_status()

            data = response.json()
            self.stats["total_fetches"] += 1

            if data and "states" in data and data["states"]:
                logger.info(f"Fetched {len(data['states'])} flights from OpenSky API")
                return data
            else:
                logger.warning("No flight data received from API")
                return None

        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching data from OpenSky API: {e}")
            self.stats["errors"] += 1
            return None

    def parse_flight_state(self, state: list, fetch_time: int) -> Dict[str, Any]:
        """
        Parse OpenSky flight state vector into structured format. These are the defined fields:

        State vector indices:
        0: icao24, 1: callsign, 2: origin_country, 3: time_position,
        4: last_contact, 5: longitude, 6: latitude, 7: baro_altitude,
        8: on_ground, 9: velocity, 10: true_track, 11: vertical_rate,
        12: sensors, 13: geo_altitude, 14: squawk, 15: spi, 16: position_source
        """
        return {
            "icao24": state[0] if state[0] else "unknown",
            "callsign": state[1].strip() if state[1] else None,
            "origin_country": state[2] if state[2] else None,
            "time_position": int(state[3]) if state[3] is not None else None,
            "last_contact": int(state[4]) if state[4] is not None else None,
            "longitude": float(state[5]) if state[5] is not None else None,
            "latitude": float(state[6]) if state[6] is not None else None,
            "baro_altitude": float(state[7]) if state[7] is not None else None,
            "on_ground": bool(state[8]) if state[8] is not None else None,
            "velocity": float(state[9]) if state[9] is not None else None,
            "true_track": float(state[10]) if state[10] is not None else None,
            "vertical_rate": float(state[11]) if state[11] is not None else None,
            "geo_altitude": float(state[13]) if state[13] is not None else None,
            "squawk": state[14] if state[14] else None,
            "spi": bool(state[15]) if state[15] is not None else None,
            "position_source": int(state[16]) if state[16] is not None else None,
            "fetch_timestamp": int(fetch_time) if fetch_time else None,
            "ingestion_time": datetime.utcnow().isoformat(),
        }

    # Callback for delivery reports. This will be called once for each message produced to indicate delivery result.
    def delivery_report(self, err, msg):
        """Callback for message delivery reports"""
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
            self.stats["errors"] += 1
        else:
            self.stats["total_sent"] += 1
            if self.stats["total_sent"] % 1000 == 0:
                logger.info(f"Delivered {self.stats['total_sent']} messages")

    # Send flight data to Kafka topic.
    def send_to_kafka(self, flight: Dict[str, Any]) -> bool:
        """Send flight data to Kafka topic with Avro serialization"""
        try:
            # Use icao24 as key for partitioning.
            key = flight["icao24"].encode("utf-8")

            # Serialize value with Avro
            serialization_context = SerializationContext(self.topic, MessageField.VALUE)
            serialized_value = self.avro_serializer(flight, serialization_context)

            # Send to Kafka
            self.producer.produce(
                topic=self.topic, key=key, value=serialized_value, on_delivery=self.delivery_report
            )

            # Poll for callbacks (non-blocking). This allows delivery reports to be processed.
            self.producer.poll(0)

            return True

        except Exception as e:
            logger.error(f"Error sending message to Kafka: {e}")
            self.stats["errors"] += 1
            return False

    def run(self):
        """Main producer loop"""
        logger.info(f"Starting OpenSky producer - fetching every {self.fetch_interval} seconds")
        logger.info(f"Publishing to topic: {self.topic}")
        logger.info("Using Avro schema with Schema Registry")

        try:
            while True:
                start_time = time.time()

                # Fetch data from OpenSky API
                data = self.fetch_flight_data()

                if data and "states" in data and data["states"]:
                    fetch_time = data.get("time", int(time.time()))

                    # Process each flight
                    for state in data["states"]:
                        if state and state[0]:  # Skip if no icao24
                            flight = self.parse_flight_state(state, fetch_time)
                            self.send_to_kafka(flight)

                    # Flush to ensure all messages are sent
                    self.producer.flush()

                    self.stats["total_flights"] += len(data["states"])
                    logger.info(f"Processed {len(data['states'])} flights")

                # Print stats periodically
                if self.stats["total_fetches"] % 10 == 0:
                    logger.info(
                        f"Stats: Fetches={self.stats['total_fetches']}, "
                        f"Flights={self.stats['total_flights']}, "
                        f"Sent={self.stats['total_sent']}, "
                        f"Errors={self.stats['errors']}"
                    )

                # Calculate sleep time to maintain interval
                elapsed = time.time() - start_time
                sleep_time = max(0, self.fetch_interval - elapsed)

                if sleep_time > 0:
                    logger.debug(f"Sleeping for {sleep_time:.2f} seconds")
                    time.sleep(sleep_time)
                else:
                    logger.warning(f"Processing took longer than interval: {elapsed:.2f}s")

        except KeyboardInterrupt:
            logger.info("Received interrupt signal, shutting down...")
        except Exception as e:
            logger.error(f"Unexpected error: {e}", exc_info=True)
        finally:
            self.cleanup()

    def cleanup(self):
        """Clean up resources"""
        logger.info("Flushing remaining messages...")
        self.producer.flush()
        logger.info(f"Final stats: {self.stats}")
        logger.info("Producer shut down cleanly")


def main():
    """Main entry point"""
    try:
        producer = OpenSkyProducer()
        producer.run()
    except Exception as e:
        logger.error(f"Failed to start producer: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
