import logging
import os
import json
import signal
import asyncio
import time
from datetime import datetime
from typing import List


# Alpaca-py for market data streaming
from alpaca.data.live.stock import StockDataStream
from alpaca.data.enums import DataFeed

# Confluent Kafka Producer
from confluent_kafka import Producer
# GCP Secret Manager
from google.cloud import secretmanager
from google.auth import default

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
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
        # For graceful shutdown
        self.killer = GracefulKiller()

        # Grab default GCP credentials (for Secret Manager)
        credentials, project_id = default()
        self.project_id = project_id

        # Load config from environment
        self.setup_config()

        # Internal tracking
        self.producer = None
        self.message_count = 0
        self.last_healthy_timestamp = time.time()

        # Alpaca StockDataStream instance
        self.stock_stream: StockDataStream | None = None

    def setup_config(self):
        """Load environment variables and define defaults."""
        # Kafka config
        self.kafka_topic = os.getenv("KAFKA_TOPIC_NAME", "stock-trades")
        self.bootstrap_server = os.getenv("KAFKA_BOOTSTRAP_SERVER", "")
        self.kafka_secret_name = os.getenv("KAFKA_SECRET_NAME", "")
        self.kafka_api_key_id = os.getenv("KAFKA_API_KEY_ID", "")

        # GCP Secret Names for Alpaca
        self.alpaca_key_secret_name = os.getenv("ALPACA_KEY_SECRET_NAME", "")
        self.alpaca_secret_secret_name = os.getenv("ALPACA_SECRET_SECRET_NAME", "")

        # Symbols to subscribe to (trades, quotes, bars)
        self.symbols = os.getenv("SYMBOLS", "AAPL,MSFT,GOOGL").split(",")

        # Reconnection & health check intervals
        self.reconnect_delay = int(os.getenv("RECONNECT_DELAY_SECONDS", "5"))
        self.health_check_interval = int(os.getenv("HEALTH_CHECK_INTERVAL", "30"))

        # Which feed to use (iex for free, sip for US full feed)
        # e.g. export ALPACA_FEED="iex"
        self.alpaca_feed = os.getenv("ALPACA_FEED", "iex").lower()

    def fetch_secret(self, secret_name: str) -> str:
        """Fetch a secret value from GCP Secret Manager."""
        client = secretmanager.SecretManagerServiceClient()
        secret_path = f"projects/{self.project_id}/secrets/{secret_name}/versions/latest"
        response = client.access_secret_version(name=secret_path)
        return response.payload.data.decode("UTF-8")

    def setup_kafka_producer(self):
        """Initialize Kafka producer with SASL credentials."""
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
        """
        Set up StockDataStream for real-time market data. 
        We'll subscribe to trades, quotes, and bars for the given symbols.
        """
        logger.info("Setting up Alpaca StockDataStream...")

        # Get Alpaca credentials from secrets
        alpaca_api_key = self.fetch_secret(self.alpaca_key_secret_name)
        alpaca_api_secret = self.fetch_secret(self.alpaca_secret_secret_name)


        # Instead of passing self.alpaca_feed as a string,
        # choose the enum based on environment variable
        from_str = self.alpaca_feed  # e.g. "iex" or "sip"

        if from_str.lower() == "sip":
            feed_enum = DataFeed.SIP
        else:
            feed_enum = DataFeed.IEX

        self.stock_stream = StockDataStream(
            api_key=alpaca_api_key,
            secret_key=alpaca_api_secret,
            feed=feed_enum,  # <-- Must be an enum value
            raw_data=False
        )

        # -------------------------------------------------------------------
        #  Define callbacks for trades, quotes, bars, and subscribe to them
        # -------------------------------------------------------------------

        @self.stock_stream.on_trades(*self.symbols)
        async def on_trades(trades):
            """Handle real-time trades for the subscribed symbols."""
            try:
                # `trades` is a list of Trade objects
                for trade in trades:
                    # Convert to dict and add processed timestamp
                    trade_dict = trade.dict()
                    trade_dict["processed_timestamp"] = datetime.utcnow().isoformat()

                    # Produce to Kafka
                    self.producer.produce(
                        self.kafka_topic,
                        key=str(time.time()),
                        value=json.dumps(trade_dict),
                    )
                self.producer.flush()

                self.message_count += len(trades)
                self.last_healthy_timestamp = time.time()
                if self.message_count % 100 == 0:
                    logger.info(f"Processed {self.message_count} trades.")

            except Exception as e:
                logger.error(f"Error processing trades: {e}")

        @self.stock_stream.on_quotes(*self.symbols)
        async def on_quotes(quotes):
            """Handle real-time quotes for the subscribed symbols."""
            try:
                for quote in quotes:
                    quote_dict = quote.dict()
                    quote_dict["processed_timestamp"] = datetime.utcnow().isoformat()

                    self.producer.produce(
                        self.kafka_topic,
                        key=str(time.time()),
                        value=json.dumps(quote_dict),
                    )
                self.producer.flush()

                self.message_count += len(quotes)
                self.last_healthy_timestamp = time.time()
                if self.message_count % 100 == 0:
                    logger.info(f"Processed {self.message_count} quotes.")

            except Exception as e:
                logger.error(f"Error processing quotes: {e}")

        @self.stock_stream.on_bars(*self.symbols)
        async def on_bars(bars):
            """Handle real-time bars for the subscribed symbols."""
            try:
                for bar in bars:
                    bar_dict = bar.dict()
                    bar_dict["processed_timestamp"] = datetime.utcnow().isoformat()

                    self.producer.produce(
                        self.kafka_topic,
                        key=str(time.time()),
                        value=json.dumps(bar_dict),
                    )
                self.producer.flush()

                self.message_count += len(bars)
                self.last_healthy_timestamp = time.time()
                if self.message_count % 100 == 0:
                    logger.info(f"Processed {self.message_count} bars.")

            except Exception as e:
                logger.error(f"Error processing bars: {e}")

    async def health_check(self):
        """Periodic health check. Warn if no messages have arrived recently."""
        while not self.killer.kill_now:
            await asyncio.sleep(self.health_check_interval)

            time_since_last_healthy = time.time() - self.last_healthy_timestamp
            if time_since_last_healthy > self.health_check_interval:
                logger.warning(
                    f"No market data received in {int(time_since_last_healthy)} seconds."
                )
                # Optionally trigger a reconnect, stop the stream, etc.
                # self.stock_stream.stop()

    async def run_market_data_stream(self):
        """Run the StockDataStream in an async task until we shut down."""
        try:
            if not self.stock_stream:
                logger.error("StockDataStream is not set up.")
                return

            logger.info("Starting Alpaca market data stream...")

            # Launch a background health checker
            health_task = asyncio.create_task(self.health_check())

            # This call blocks until we stop the stream or an error occurs
            await self.stock_stream.run()

            # Cancel the health check when done
            health_task.cancel()

        except Exception as e:
            logger.error(f"Stream error: {e}")
            if not self.killer.kill_now:
                logger.info(f"Reconnecting in {self.reconnect_delay} seconds...")
                await asyncio.sleep(self.reconnect_delay)
                await self.run_market_data_stream()

    async def run(self):
        """Main entry point to set up Kafka, set up the stream, and run forever."""
        self.setup_kafka_producer()
        self.setup_alpaca_stream()

        try:
            await self.run_market_data_stream()
        finally:
            if self.producer:
                self.producer.flush()
            logger.info(f"Final message count: {self.message_count}")
            logger.info("Exiting MarketDataConsumer.")

def main():
    consumer = MarketDataConsumer()
    asyncio.run(consumer.run())

if __name__ == "__main__":
    main()
