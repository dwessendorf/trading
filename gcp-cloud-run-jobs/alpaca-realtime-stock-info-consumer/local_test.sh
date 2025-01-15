# Kafka Settings
export KAFKA_TOPIC_NAME=stock-info
export KAFKA_BOOTSTRAP_SERVER=SASL_SSL://pkc-75m1o.europe-west3.gcp.confluent.cloud:9092
export KAFKA_SECRET_NAME=gcp-confluent-key
export KAFKA_API_KEY_ID=G2K3DHWITKHPZUPP
export GCP_CONFLUENT_KEY=DUhW+8HvhVyTM+8A4Wmn6u0wbmKqgpHEQIwC0vwhIvm3ydYpkA3acYD+wkDZBaoh
# Alpaca Settings
export ALPACA_API_KEY=PK7PDL4ZGDFP685BJG8L
export ALPACA_API_SECRET=B8MXnDp8sud8eGjEMXVeS9vhHHoSMmUtFDCAlD8U
export ALPACA_KEY_SECRET_NAME=alpaca-api-key
export ALPACA_SECRET_SECRET_NAME=alpaca-api-secret
export SYMBOLS=AAPL,MSFT,GOOGL
export ALPACA_FEED=iex
# Application Settings
export RECONNECT_DELAY_SECONDS=5
export HEALTH_CHECK_INTERVAL=30
export LOCAL_DEV=TRUE
python3 main.py

