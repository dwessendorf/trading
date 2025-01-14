import logging
import os
import json
import signal
import asyncio
from datetime import datetime
import time
from typing import Optional

# Alpaca-Py import for trade update streams
from alpaca.trading.stream import TradingStream
# Confluent Kafka Producer
from confluent_kafka import Producer
# GCP Secret Manager
from google.cloud import secretmanager
from google.auth import default

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("market-stream-consumer")

class GracefulKiller:
    """Handle graceful shutdown on SIGINT or SIGTERM."""
    def __init__(self):
        self.kill_now = False
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, *args):
        logger.info("Received shutdown signal...")
        self.kill_now = True

class MarketDataConsumer:
    def __init__(self):
        # For handling shutdown signals
        self.killer = GracefulKiller()

        # Grab default GCP credentials, plus project_id
        credentials, project_id = default()
        self.project_id = project_id

        # Load environment variables
        self.setup_config()

        # Kafka producer
        self.producer = None

        # Track how many messages we’ve produced
        self.message_count = 0
        self.last_healthy_timestamp = time.time()

        # Alpaca trade stream object (assigned later in setup_alpaca_stream)
        self.trade_stream: Optional[TradingStream] = None

    def setup_config(self):
        """Load configuration from environment variables."""
        # Kafka config
        self.kafka_topic = os.getenv("KAFKA_TOPIC_NAME")
        self.bootstrap_server = os.getenv("KAFKA_BOOTSTRAP_SERVER")
        self.kafka_secret_name = os.getenv("KAFKA_SECRET_NAME")
        self.kafka_api_key_id = os.getenv("KAFKA_API_KEY_ID")

        # GCP secret names for Alpaca credentials
        self.alpaca_key_secret_name = os.getenv("ALPACA_KEY_SECRET_NAME")
        self.alpaca_secret_secret_name = os.getenv("ALPACA_SECRET_SECRET_NAME")

        # Reconnect logic
        self.reconnect_delay = int(os.getenv("RECONNECT_DELAY_SECONDS", "5"))

        # Health-check interval
        self.health_check_interval = int(os.getenv("HEALTH_CHECK_INTERVAL", "30"))

        # Whether to use Alpaca paper trading or live trading
        # (Default = True → paper mode)
        self.alpaca_is_paper = os.getenv("ALPACA_PAPER_TRADING", "true").lower() in ["true", "1", "yes"]

    def fetch_secret(self, secret_name: str) -> str:
        """Fetch a secret value from GCP Secret Manager."""
        client = secretmanager.SecretManagerServiceClient()
        secret_path = f"projects/{self.project_id}/secrets/{secret_name}/versions/latest"
        response = client.access_secret_version(name=secret_path)
        return response.payload.data.decode("UTF-8")

    def setup_kafka_producer(self):
        """Initialize Kafka producer with retry logic."""
        logger.info("Setting up Kafka producer...")
        kafka_api_key_secret = self.fetch_secret(self.kafka_secret_name)

        kafka_config = {
            "bootstrap.servers": self.bootstrap_server,
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "PLAIN",
            "sasl.username": self.kafka_api_key_id,
            "sasl.password": kafka_api_key_secret,
            # Reliable delivery configs
            "enable.idempotence": True,
            "acks": "all",
            "retries": 10,
            "retry.backoff.ms": 1000,
            "delivery.timeout.ms": 120000,
        }

        self.producer = Producer(kafka_config)
        logger.info("Kafka producer created successfully.")

    def setup_alpaca_stream(self):
        """Set up Alpaca trading stream to receive trade updates (order fill events, etc.)."""
        logger.info("Setting up Alpaca trade update stream...")

        # Fetch Alpaca credentials from GCP Secret Manager
        alpaca_api_key = self.fetch_secret(self.alpaca_key_secret_name)
        alpaca_api_secret = self.fetch_secret(self.alpaca_secret_secret_name)

        # Create the TradingStream
        # paper=True uses paper trading environment, paper=False uses live trading
        self.trade_stream = TradingStream(
            key_id=alpaca_api_key,
            secret_key=alpaca_api_secret,
            paper=self.alpaca_is_paper
        )

        # Define the callback for trade updates
        @self.trade_stream.on("trade_updates")
        async def handle_trade_update(data):
            """
            `data` is an event describing a trade update (e.g. fill, partial fill).
            We'll send it to Kafka.
            """
            try:
                event_dict = data.__dict__ if hasattr(data, "__dict__") else dict(data)
                # Add a processed timestamp
                event_dict["processed_timestamp"] = datetime.utcnow().isoformat()

                # Convert to JSON string
                payload_str = json.dumps(event_dict)

                # Produce to Kafka
                self.producer.produce(
                    self.kafka_topic,
                    key=str(time.time()),
                    value=payload_str
                )
                # Flush ensures delivery (for small throughput)
                self.producer.flush()

                self.message_count += 1
                self.last_healthy_timestamp = time.time()

                if self.message_count % 100 == 0:
                    logger.info(f"Processed {self.message_count} trade updates.")

            except Exception as exc:
                logger.error(f"Error in handle_trade_update: {exc}")

    async def health_check(self):
        """
        Periodically check that we're receiving trade updates. 
        If too long passes without updates, we can handle it (warn, reconnect, etc.).
        """
        while not self.killer.kill_now:
            await asyncio.sleep(self.health_check_interval)

            time_since_last_healthy = time.time() - self.last_healthy_timestamp
            if time_since_last_healthy > self.health_check_interval:
                logger.warning(
                    f"No trade updates in {int(time_since_last_healthy)} seconds. " 
                    f"Possible stream issue?"
                )
                # If needed, we can stop or force a reconnect:
                # self.trade_stream.stop()
                # break

    async def run_trade_stream(self):
        """
        Run the Alpaca trade stream until kill signal is received. 
        The TradingStream library has built-in reconnection logic.
        """
        try:
            # Start the trade stream (non-blocking)
            logger.info("Starting Alpaca trade update stream...")

            # Launch a background health checker
            health_task = asyncio.create_task(self.health_check())

            # This call blocks until the stream is stopped or an exception is raised.
            await self.trade_stream.run()

            # Cancel health check when the stream ends
            health_task.cancel()

        except Exception as e:
            logger.error(f"Trade stream error: {e}")
            # If something breaks, you can attempt a re-init or wait.
            if not self.killer.kill_now:
                logger.info(f"Reconnecting in {self.reconnect_delay} seconds...")
                await asyncio.sleep(self.reconnect_delay)
                await self.run_trade_stream()

    async def run(self):
        """Main entry point to set up everything and run the streaming loop."""
        self.setup_kafka_producer()
        self.setup_alpaca_stream()

        try:
            await self.run_trade_stream()
        finally:
            if self.producer:
                self.producer.flush()
            logger.info(f"Final message count: {self.message_count}")
            logger.info("Exiting MarketDataConsumer.")

def main():
    consumer = MarketDataConsumer()

    # Use asyncio.run to handle the async flow
    asyncio.run(consumer.run())

if __name__ == "__main__":
    main()
