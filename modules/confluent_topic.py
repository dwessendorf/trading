import pulumi
from pulumi_confluentcloud import KafkaCluster, ServiceAccount, KafkaTopic, ApiKey

def deploy_confluent_topic(config):
    # Create a Confluent Kafka Cluster
    
    # Create a Kafka Topic
    kafka_topic = KafkaTopic(
        resource_name=config["topic_name"],
        topic_name=config["topic_name"],
        kafka_cluster={
            "id": config["kafka_cluster_id"],
        },
        rest_endpoint=config["kafka_cluster_rest_endpoint"],
        partitions_count=3,
        config={
            "cleanup.policy": "delete",
            "retention.ms": "604800000",  # 7 days
        },
        credentials={
            "key": config["api_key_id"],
            "secret": config["api_key_secret"],
        }
    )


    # Export bootstrap servers and other details
    pulumi.export("kafka_topic_id", kafka_topic.id)
    
    #Return bootstrap servers and API Key details
    return {
        "kafka_topic_id": kafka_topic.id,
    }

