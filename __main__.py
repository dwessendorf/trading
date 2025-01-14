import pulumi
from modules.confluent_cluster_gcp import deploy_confluent_cluster_on_gcp
from modules.confluent_topic import deploy_confluent_topic
from modules.gcp_cloud_run_job import deploy_cloud_run_job_with_secrets

# Pulumi Config
config = pulumi.Config()
confluent_environment_id = config.require("environment_id")
apikey_owner_id = config.require("apikey_owner_id")

# Confluent Cluster Configuration
gcp_confluent_config = {
    "cluster_name": config.require("gcp_cluster_name"),
    "region": config.require("gcp_region"),
    "environment_id": confluent_environment_id,
    "apikey_owner_id": apikey_owner_id,
}
gcp_kafka_details = deploy_confluent_cluster_on_gcp(gcp_confluent_config)

# Kafka Topic Configuration
gcp_topic_config = {
    "topic_name": config.require("stock_info_topic_name"),
    "kafka_cluster_id": gcp_kafka_details["cluster_id"],
    "api_key_id": gcp_kafka_details["api_key_id"],
    "api_key_secret": gcp_kafka_details["api_key_secret"],
    "bootstrap_servers": gcp_kafka_details["bootstrap_servers"],
    "kafka_cluster_rest_endpoint": gcp_kafka_details["rest_endpoint"]
}
gcp_kafka_topic_details = deploy_confluent_topic(gcp_topic_config)

# Cloud Run Job Configuration
cloud_run_job_config = {
    "job_name": config.require("gcp_job_name"),
    "region": config.require("gcp_region"),
    "project": config.get("gcp_project") or "partner-engineering",
    "kafka_topic_name": config.require("stock_info_topic_name"),
    "kafka_secret_name": config.require("kafka_secret_name"),
    "alpaca_key_secret_name": config.require("alpaca_key_secret_name"),
    "alpaca_secret_secret_name": config.require("alpaca_secret_secret_name"),
    "alpaca_api_key": config.require("alpaca_api_key"),
    "alpaca_api_secret": config.require("alpaca_api_secret"),
    "stock_symbols": config.require("stock_symbols"),
    "schedule": config.get("job_schedule") or "0 */4 * * *",  # Every 4 hours by default
    "cpu": config.get("job_cpu") or "1",
    "memory": config.get("job_memory") or "2Gi",
    "timeout_seconds": config.get("job_timeout") or "86400",  # 24 hours by default
    "max_retries": config.get("job_max_retries") or "3",
    "reconnect_delay_seconds": config.get("reconnect_delay_seconds") or "5",
    "health_check_interval": config.get("health_check_interval") or "30",
    "alpaca_feed": config.get("alpaca_feed") or "idx"
}

deploy_cloud_run_job_with_secrets(
    config=cloud_run_job_config,
    source_dir="./cloud-run/market-stream-consumer",
    kafka_details=gcp_kafka_details,
)