import logging
import os
import json
import signal
import asyncio
import time
from datetime import datetime
from typing import List

from alpaca.data.live.stock import StockDataStream
from alpaca.data.models.quotes import Quote
from alpaca.data.models.trades import Trade
from alpaca.data.models.bars import Bar
from alpaca.data.enums import DataFeed
from confluent_kafka import Producer
from google.cloud import secretmanager
from google.auth import default
import re
from datetime import datetime

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
        self.killer = GracefulKiller()
        credentials, project_id = default()
        self.project_id = project_id
        self.setup_config()
        self.producer = None
        self.message_count = 0
        self.last_healthy_timestamp = time.time()
        self.stock_stream: StockDataStream | None = None
        self._event_loop = None

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
        print(kafka_config)
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
            # For quotes, the callback receives tuples: (symbol, quote_object)
            self.stock_stream.subscribe_quotes(self.handle_quotes, symbol)
            self.stock_stream.subscribe_bars(self.handle_bars, symbol)

    async def handle_trades(self, trades):
        print(trades)
        # try:
        #     # Depending on your alpaca-py version, trades might already be objects;
        #     # if needed, add unpacking similar to quotes.
        #     for trade in trades:
        #         # If trade is a tuple, uncomment the following line:
        #         # _, trade_obj = trade
        parsed_trade = {}
        trade_obj = self.getTrade(trades)
        # or replace with unpacking if necessary
        parsed_trade["processed_timestamp"] = datetime.utcnow().isoformat()
        parsed_trade["trade"] = trade_obj.model_dump_json()

        print(parsed_trade)
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

        # except Exception as e:
        #     logger.error(f"Error processing trades: {e}")



    async def handle_quotes(self, quotes):

        try:
            quote_object = self.getQuote(quotes)
            parsed_quote = {}
            parsed_quote["processed_timestamp"] = str(datetime.utcnow().isoformat())
            parsed_quote["quote"] = quote_object.model_dump_json()

            print(type(parsed_quote))
            print(parsed_quote)

            # Produce the parsed dictionary to Kafka as JSON.
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
            # # Depending on your version, bars may or may not be tuples.
            # # If they are tuples, unpack similarly to quotes.
            # for symbol, bar_obj in bars:
            bar_dict = self.getBar(bars)
            parsed_bar = {}
            
            parsed_bar["processed_timestamp"] = datetime.utcnow().isoformat()
            parsed_bar["bar"] = bar_dict.model_dump_json()  
            print(parsed_bar)              

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
            logger.error(f"Error processing bars: {e}")

    async def health_check(self):
        while not self.killer.kill_now:
            await asyncio.sleep(self.health_check_interval)
            time_since_last_healthy = time.time() - self.last_healthy_timestamp

            if time_since_last_healthy > self.health_check_interval:
                logger.warning(
                    f"No market data received in {int(time_since_last_healthy)} seconds."
                )

    async def run_market_data_stream(self):
        try:
            if not self.stock_stream:
                logger.error("StockDataStream is not set up.")
                return

            logger.info("Starting Alpaca market data stream...")
            health_task = asyncio.create_task(self.health_check())

            try:
                await self.stock_stream._run_forever()
            except Exception as e:
                logger.error(f"Stream error: {e}")
            finally:
                health_task.cancel()
                try:
                    await health_task
                except asyncio.CancelledError:
                    pass

        except Exception as e:
            logger.error(f"Stream error: {e}")
            if not self.killer.kill_now:
                logger.info(f"Reconnecting in {self.reconnect_delay} seconds...")
                await asyncio.sleep(self.reconnect_delay)
                await self.run_market_data_stream()

    async def run(self):
        """Main entry point."""
        logger.info("Starting MarketDataConsumer...")
        self.setup_kafka_producer()
        await self.setup_alpaca_stream()

        try:
            await self.run_market_data_stream()
        finally:
            if self.producer:
                self.producer.flush()
            if self.stock_stream:
                await self.stock_stream.close()
            logger.info(f"Final message count: {self.message_count}")
            logger.info("Exiting MarketDataConsumer.")

def main():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    consumer = MarketDataConsumer()
    try:
        loop.run_until_complete(consumer.run())
    finally:
        loop.close()

if __name__ == "__main__":
    main()
