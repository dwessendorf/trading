import asyncio
import json
import logging
import os
import signal
import time
from datetime import datetime

from alpaca.data.live.stock import StockDataStream
from alpaca.data.models.quotes import Quote
from alpaca.data.models.trades import Trade
from alpaca.data.models.bars import Bar
from alpaca.data.enums import DataFeed
from confluent_kafka import Producer
from google.cloud import secretmanager
from google.auth import default

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("market-stream-consumer")


class MarketDataConsumer:
    def __init__(self):
        credentials, project_id = default()
        self.project_id = project_id
        self.setup_config()
        self.producer = None
        self.message_count = 0
        self.last_healthy_timestamp = time.time()
        self.stock_stream: StockDataStream | None = None

    def setup_config(self):
        """Load environment variables and define defaults."""
        self.kafka_topic = os.getenv("KAFKA_TOPIC_NAME", "stock-trades")
        self.bootstrap_server = os.getenv("KAFKA_BOOTSTRAP_SERVER", "")
        self.kafka_secret_name = os.getenv("KAFKA_SECRET_NAME", "")
        self.kafka_api_key_id = os.getenv("KAFKA_API_KEY_ID", "")
        self.alpaca_key_secret_name = os.getenv("ALPACA_KEY_SECRET_NAME", "")
        self.alpaca_secret_secret_name = os.getenv("ALPACA_SECRET_SECRET_NAME", "")
        self.symbols = os.getenv("SYMBOLS", "AAPL,MSFT,GOOGL").split(",")
        self.reconnect_delay = int(os.getenv("RECONNECT_DELAY_SECONDS", "5"))
        self.health_check_interval = int(os.getenv("HEALTH_CHECK_INTERVAL", "30"))
        self.alpaca_feed = os.getenv("ALPACA_FEED", "iex").lower()
        self.alpaca_websocket_override_url = os.getenv("ALPACA_WEBSOCKET_OVERRIDE_URL", "")

    def fetch_secret(self, secret_name: str) -> str:
        if os.getenv('LOCAL_DEV'):
            # For local development, use environment variables directly
            secret_name_upper = secret_name.upper().replace('-', '_')
            return os.getenv(secret_name_upper, '')
        
        client = secretmanager.SecretManagerServiceClient()
        secret_path = f"projects/{self.project_id}/secrets/{secret_name}/versions/latest"
        response = client.access_secret_version(name=secret_path)
        return response.payload.data.decode("UTF-8")

    def setup_kafka_producer(self):
        logger.info("Setting up Kafka producer...")
        kafka_api_key_secret = self.fetch_secret(self.kafka_secret_name)
        kafka_config = {
            "bootstrap.servers": self.bootstrap_server,
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "PLAIN",
            "sasl.username": self.kafka_api_key_id,
            "sasl.password": kafka_api_key_secret,
            "enable.idempotence": True,
            "acks": "all",
            "retries": 10,
            "retry.backoff.ms": 1000,
            "delivery.timeout.ms": 120000,
        }
        logger.info(f"Kafka config: {kafka_config}")
        self.producer = Producer(kafka_config)
        logger.info("Kafka producer created successfully.")

    def getQuote(self, quotestr) -> Quote:
        return quotestr

    def getTrade(self, tradestr) -> Trade:
        return tradestr

    def getBar(self, barstr) -> Bar:
        return barstr

    async def setup_alpaca_stream(self):
        """Create and initialize the StockDataStream and subscribe to data."""
        logger.info("Setting up Alpaca StockDataStream...")
        alpaca_api_key = self.fetch_secret(self.alpaca_key_secret_name)
        alpaca_api_secret = self.fetch_secret(self.alpaca_secret_secret_name)
        feed_enum = DataFeed.SIP if self.alpaca_feed == "sip" else DataFeed.IEX

        logger.info(f"Using feed: {feed_enum}")
        logger.info(f"Symbols: {self.symbols}")
        if self.alpaca_websocket_override_url:
            self.stock_stream = StockDataStream(
                api_key=alpaca_api_key,
                secret_key=alpaca_api_secret,
                feed=feed_enum,
                raw_data=False,
                url_override=self.alpaca_websocket_override_url,
            )
        else:
            self.stock_stream = StockDataStream(
                api_key=alpaca_api_key,
                secret_key=alpaca_api_secret,
                feed=feed_enum,
                raw_data=False
            )

        # Subscribe to data streams for each symbol
        for symbol in self.symbols:
            logger.info(f"Subscribing to data for {symbol}")
            self.stock_stream.subscribe_trades(self.handle_trades, symbol)
            self.stock_stream.subscribe_quotes(self.handle_quotes, symbol)
            self.stock_stream.subscribe_bars(self.handle_bars, symbol)

    async def handle_trades(self, trades):
        try:
            parsed_trade = {}
            trade_obj = self.getTrade(trades)
            parsed_trade["processed_timestamp"] = datetime.utcnow().isoformat()
            parsed_trade["trade"] = trade_obj.model_dump_json()

            logger.debug(f"Trade parsed: {parsed_trade}")
            self.producer.produce(
                self.kafka_topic,
                key=str(time.time()),
                value=json.dumps(parsed_trade),
            )
            self.producer.flush()

            self.message_count += 1
            self.last_healthy_timestamp = time.time()

            if self.message_count % 100 == 0:
                logger.info(f"Processed {self.message_count} trades.")

        except Exception as e:
            logger.error(f"Error processing trades: {e}", exc_info=True)

    async def handle_quotes(self, quotes):
        try:
            quote_object = self.getQuote(quotes)
            parsed_quote = {}
            parsed_quote["processed_timestamp"] = datetime.utcnow().isoformat()
            parsed_quote["quote"] = quote_object.model_dump_json()

            logger.debug(f"Quote parsed: {parsed_quote}")
            self.producer.produce(
                self.kafka_topic,
                key=str(time.time()),
                value=json.dumps(parsed_quote),
            )
            self.producer.flush()
            self.message_count += 1
            self.last_healthy_timestamp = time.time()

            if self.message_count % 100 == 0:
                logger.info(f"Processed {self.message_count} quotes.")
        except Exception as e:
            logger.error(f"Error processing quotes: {e}", exc_info=True)
            logger.debug(f"Quote data that caused error: {quotes}")

    async def handle_bars(self, bars):
        try:
            bar_dict = self.getBar(bars)
            parsed_bar = {}
            parsed_bar["processed_timestamp"] = datetime.utcnow().isoformat()
            parsed_bar["bar"] = bar_dict.model_dump_json()
            logger.debug(f"Bar parsed: {parsed_bar}")

            self.producer.produce(
                self.kafka_topic,
                key=str(time.time()),
                value=json.dumps(parsed_bar),
            )
            self.producer.flush()

            self.message_count += 1
            self.last_healthy_timestamp = time.time()

            if self.message_count % 100 == 0:
                logger.info(f"Processed {self.message_count} bars.")

        except Exception as e:
            logger.error(f"Error processing bars: {e}", exc_info=True)

    async def health_check(self):
        while True:
            await asyncio.sleep(self.health_check_interval)
            time_since_last_healthy = time.time() - self.last_healthy_timestamp
            if time_since_last_healthy > self.health_check_interval:
                logger.warning(
                    f"No market data received in {int(time_since_last_healthy)} seconds."
                )

    async def run_market_data_stream(self):
        if not self.stock_stream:
            logger.error("StockDataStream is not set up.")
            return

        logger.info("Starting Alpaca market data stream...")
        # Run both the stream and health check as tasks for cancellation
        health_task = asyncio.create_task(self.health_check())
        stream_task = asyncio.create_task(self.stock_stream._run_forever())

        try:
            # Wait for either task to complete (which should normally be the stream)
            await asyncio.wait(
                [stream_task, health_task],
                return_when=asyncio.FIRST_COMPLETED,
            )
        except asyncio.CancelledError:
            logger.info("Stream cancellation requested.")
            raise
        finally:
            health_task.cancel()
            stream_task.cancel()
            await asyncio.gather(health_task, stream_task, return_exceptions=True)

    async def run(self):
        """Main entry point."""
        logger.info("Starting MarketDataConsumer...")
        self.setup_kafka_producer()
        await self.setup_alpaca_stream()

        try:
            await self.run_market_data_stream()
        except asyncio.CancelledError:
            logger.info("MarketDataConsumer run cancelled.")
        finally:
            if self.producer:
                self.producer.flush()
            if self.stock_stream:
                await self.stock_stream.close()
            logger.info(f"Final message count: {self.message_count}")
            logger.info("Exiting MarketDataConsumer.")


async def shutdown(signal_name, loop):
    logger.info(f"Received exit signal {signal_name}. Initiating shutdown...")
    tasks = [t for t in asyncio.all_tasks(loop) if t is not asyncio.current_task()]
    logger.info(f"Cancelling {len(tasks)} outstanding tasks.")
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    logger.info("All tasks have been cancelled.")


def main():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # Register signal handlers for graceful shutdown
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(
            sig, lambda s=sig: asyncio.create_task(shutdown(s.name, loop))
        )

    consumer = MarketDataConsumer()
    try:
        loop.run_until_complete(consumer.run())
        # Short wait to allow shutdown tasks to finish cancellation
        loop.run_until_complete(asyncio.sleep(0.1))
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received. Shutting down.")
    finally:
        # Shutdown asynchronous generators if any exist
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()


if __name__ == "__main__":
    main()
