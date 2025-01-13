import logging
import os
import json
from datetime import datetime
from confluent_kafka import Producer
from google.cloud import secretmanager
from google.auth import default
import requests
from typing import Dict

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("stock-data-producer")

def fetch_secret(secret_name: str, project_id: str) -> str:
    """Fetch a secret value from GCP Secret Manager."""
    client = secretmanager.SecretManagerServiceClient()
    secret_path = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    response = client.access_secret_version(name=secret_path)
    return response.payload.data.decode("UTF-8")

def fetch_stock_data(symbol: str, api_key: str) -> Dict:
    """Fetches real-time stock data from Alpha Vantage."""
    url = f'https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={symbol}&apikey={api_key}'
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to fetch data: {response.status_code}")

def generate_response(request):
    """Cloud Function entry point."""
    logger.info("Initializing the Cloud Function")
    
    # Get project credentials
    credentials, project_id = default()
    
    # Fetch configurations from environment variables
    kafka_topic_name = os.getenv("KAFKA_TOPIC_NAME")
    bootstrap_server = os.getenv("KAFKA_BOOTSTRAP_SERVER")
    kafka_secret_name = os.getenv("KAFKA_SECRET_NAME")
    kafka_api_key_id = os.getenv("KAFKA_API_KEY_ID")
    alpha_vantage_secret_name = os.getenv("ALPHA_VANTAGE_SECRET_NAME")
    default_symbol = os.getenv("DEFAULT_STOCK_SYMBOL", "IBM")
    
    try:
        # Handle both HTTP requests and direct invocation
        symbol = default_symbol
        if request and hasattr(request, 'get_json'):
            try:
                request_json = request.get_json(silent=True)
                if request_json and 'symbol' in request_json:
                    symbol = request_json['symbol']
            except Exception as e:
                logger.warning(f"Failed to parse request JSON: {e}")
                # Continue with default symbol
        
        logger.info(f"Processing stock data for symbol: {symbol}")
        
        # Fetch secrets from Secret Manager
        kafka_api_key_secret = fetch_secret(kafka_secret_name, project_id)
        alpha_vantage_api_key = fetch_secret(alpha_vantage_secret_name, project_id)
        
        # Configure Kafka producer
        kafka_config = {
            'bootstrap.servers': bootstrap_server,
            'security.protocol': "SASL_SSL",
            'sasl.mechanism': "PLAIN",
            'sasl.username': kafka_api_key_id,
            'sasl.password': kafka_api_key_secret
        }
        
        producer = Producer(kafka_config)
        
        
        # Fetch stock data
        stock_data = fetch_stock_data(symbol, alpha_vantage_api_key)
        
        # Enrich the data with timestamp
        stock_data['timestamp'] = datetime.now().isoformat()
        
        # Prepare the payload
        payload = json.dumps(stock_data)
        
        # Produce to Kafka topic
        producer.produce(
            kafka_topic_name,
            key=symbol,
            value=payload
        )
        
        # Wait for any outstanding messages to be delivered
        producer.flush()
        
        return {
            'status': 'success',
            'message': f'Stock data for {symbol} processed successfully'
        }
        
    except Exception as e:
        logger.exception("Unexpected error in the stock data producer")
        return {'status': 'error', 'message': str(e)}, 500

if __name__ == "__main__":
    try:
        generate_response(None)
    except Exception as e:
        logger.critical("Unhandled exception in main execution", exc_info=True)