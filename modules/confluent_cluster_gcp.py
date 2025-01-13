import pulumi
from pulumi_confluentcloud import KafkaCluster, ServiceAccount, KafkaTopic, ApiKey

def deploy_confluent_cluster_on_gcp(config):
    # Create a Confluent Kafka Cluster
    
    
    kafka_cluster = KafkaCluster(
        resource_name="GCP_Kafka_Cluster",
        display_name=config["cluster_name"],
        availability="SINGLE_ZONE",
        cloud="GCP",
        region=config["region"],
        basic={},     
        environment={
            "id": config["environment_id"],
        },

    )

        # Create a Confluent Cloud API Key
    api_key = ApiKey("myApiKey",
        owner={
            "id": config["apikey_owner_id"],  # Replace with the actual owner ID
            "kind": "User",  # or other appropriate kind
            "apiVersion": "iam/v2"
        },
        display_name="My API Key",
        managed_resource={
            "id": kafka_cluster.id,  # Replace with the actual managed resource ID
            "kind": "Cluster",  # or other appropriate kind
            "apiVersion": "cmk/v2",
            "environment": {
                "id": config["environment_id"],  # Replace with the actual environment ID
            }
        },
        description="This is my Confluent Cloud API key",
        disable_wait_for_ready=False
    )

    # # Create a Kafka Topic
    # kafka_topic = KafkaTopic(
    #     resource_name="GCP_Kafka_Topic",
    #     topic_name=config["topic_name"],
    #     kafka_cluster={
    #         "id": kafka_cluster.id,
    #     },
    #     rest_endpoint=kafka_cluster.rest_endpoint,
    #     partitions_count=3,
    #     config={
    #         "cleanup.policy": "delete",
    #         "retention.ms": "604800000",  # 7 days
    #     },
    #     credentials={
    #         "key": api_key.id,
    #         "secret": api_key.secret,
    #     }
    # )


    # Export bootstrap servers and other details
    bootstrap_servers = kafka_cluster.bootstrap_endpoint  # REST endpoint acts as bootstrap servers
    pulumi.export("gcp_bootstrap_servers", bootstrap_servers)
    pulumi.export("gcp_kafka_cluster_id", kafka_cluster.id)
    pulumi.export("gcp_api_key_id", api_key.id)
    pulumi.export("gcp_api_key_secret", api_key.secret)

    #Return bootstrap servers and API Key details
    return {
        "bootstrap_servers": bootstrap_servers,
        "api_key_id": api_key.id,
        "api_key_secret": api_key.secret,
        "cluster_id": kafka_cluster.id,
        "rest_endpoint": kafka_cluster.rest_endpoint
    }

