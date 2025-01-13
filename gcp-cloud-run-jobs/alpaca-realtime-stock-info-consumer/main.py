import logging
import os
import json
import signal
import asyncio
from datetime import datetime
from confluent_kafka import Producer
from google.cloud import secretmanager
from google.auth import default
import websockets
import time
from typing import List

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
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
        self.message_count = 0
        self.last_healthy_timestamp = time.time()
        self.producer = None
        
    def setup_config(self):
        """Load configuration from environment variables."""
        self.kafka_topic = os.getenv("KAFKA_TOPIC_NAME")
        self.bootstrap_server = os.getenv("KAFKA_BOOTSTRAP_SERVER")
        self.kafka_secret_name = os.getenv("KAFKA_SECRET_NAME")
        self.kafka_api_key_id = os.getenv("KAFKA_API_KEY_ID")
        self.alpaca_key_secret_name = os.getenv("ALPACA_KEY_SECRET_NAME")
        self.alpaca_secret_secret_name = os.getenv("ALPACA_API_SECRET_SECRET_NAME")
        self.symbols = os.getenv("SYMBOLS", "AAPL,MSFT,GOOGL").split(",")
        self.reconnect_delay = int(os.getenv("RECONNECT_DELAY_SECONDS", "5"))
        self.health_check_interval = int(os.getenv("HEALTH_CHECK_INTERVAL", "30"))
        
    def fetch_secret(self, secret_name: str) -> str:
        """Fetch a secret value from GCP Secret Manager."""
        client = secretmanager.SecretManagerServiceClient()
        secret_path = f"projects/{self.project_id}/secrets/{secret_name}/versions/latest"
        response = client.access_secret_version(name=secret_path)
        return response.payload.data.decode("UTF-8")

    def setup_kafka_producer(self):
        """Initialize Kafka producer with retry logic."""
        kafka_api_key_secret = self.fetch_secret(self.kafka_secret_name)
        
        kafka_config = {
            'bootstrap.servers': self.bootstrap_server,
            'security.protocol': "SASL_SSL",
            'sasl.mechanism': "PLAIN",
            'sasl.username': self.kafka_api_key_id,
            'sasl.password': kafka_api_key_secret,
            # Add reliable delivery configs
            'enable.idempotence': True,
            'acks': 'all',
            'retries': 10,
            'retry.backoff.ms': 1000,
            'delivery.timeout.ms': 120000
        }
        
        self.producer = Producer(kafka_config)

    async def health_check(self):
        """Monitor stream health and connection status."""
        while not self.killer.kill_now:
            time_since_last_healthy = time.time() - self.last_healthy_timestamp
            if time_since_last_healthy > self.health_check_interval:
                logger.warning(f"No messages received in {time_since_last_healthy} seconds")
                return False
            await asyncio.sleep(self.health_check_interval)
        return True

    async def stream_market_data(self):
        """Main streaming function with reconnection logic."""
        websocket_url = "wss://stream.data.alpaca.markets/v2/iex"
        
        # Fetch Alpaca credentials
        alpaca_api_key = self.fetch_secret(self.alpaca_key_secret_name)
        alpaca_api_secret = self.fetch_secret(self.alpaca_secret_secret_name)
        
        while not self.killer.kill_now:
            try:
                async with websockets.connect(websocket_url) as websocket:
                    # Authentication
                    auth_message = {
                        "action": "auth",
                        "key": alpaca_api_key,
                        "secret": alpaca_api_secret
                    }
                    await websocket.send(json.dumps(auth_message))
                    await websocket.recv()

                    # Subscribe to streams
                    subscribe_message = {
                        "action": "subscribe",
                        "trades": self.symbols,
                        "quotes": self.symbols,
                        "bars": self.symbols
                    }
                    await websocket.send(json.dumps(subscribe_message))
                    await websocket.recv()
                    
                    # Start health check
                    health_task = asyncio.create_task(self.health_check())
                    
                    while not self.killer.kill_now:
                        try:
                            message = await asyncio.wait_for(websocket.recv(), timeout=10.0)
                            data = json.loads(message)
                            
                            # Add timestamp and produce to Kafka
                            for item in data:
                                item['processed_timestamp'] = datetime.now().isoformat()
                            
                            self.producer.produce(
                                self.kafka_topic,
                                key=str(time.time()),
                                value=json.dumps(data)
                            )
                            self.producer.flush()
                            
                            self.message_count += 1
                            self.last_healthy_timestamp = time.time()
                            
                            if self.message_count % 1000 == 0:
                                logger.info(f"Processed {self.message_count} messages")
                            
                        except asyncio.TimeoutError:
                            continue
                        except Exception as e:
                            logger.error(f"Error processing message: {e}")
                            if not await self.health_check():
                                break
                    
                    # Cancel health check task
                    health_task.cancel()
                    
            except Exception as e:
                logger.error(f"WebSocket error: {e}")
                if not self.killer.kill_now:
                    logger.info(f"Reconnecting in {self.reconnect_delay} seconds...")
                    await asyncio.sleep(self.reconnect_delay)

        logger.info("Shutting down stream consumer...")

    async def run(self):
        """Main entry point."""
        try:
            self.setup_kafka_producer()
            await self.stream_market_data()
        finally:
            if self.producer:
                self.producer.flush()
                logger.info(f"Final message count: {self.message_count}")

def main():
    """Container entry point."""
    consumer = MarketDataConsumer()
    asyncio.run(consumer.run())

if __name__ == "__main__":
    main()
