import os
import pulumi
import pulumi_gcp as gcp
from pulumi_gcp import cloudbuild, cloudscheduler, secretmanager, serviceaccount
# If you prefer a shorter alias, you could do:
# from pulumi_gcp.cloudrun import v2 as cloudrunv2

def deploy_cloud_run_job_with_secrets(config: dict, source_dir: str, kafka_details: dict):
    """Deploy a Cloud Run job (v2) with associated secrets and a Cloud Scheduler trigger."""

    job_name = config["job_name"]
    # Use only 18 characters for certain GCP resource name requirements
    job_name_18 = job_name[:18]

    # Step 1: Create a Service Account
    service_account = serviceaccount.Account(
        f"{job_name_18}-sa",
        display_name=f"{job_name}-gcp-function-serviceaccount",
    )

    # Create secrets for Alpaca credentials
    alpaca_key_secret = secretmanager.Secret(
        config["alpaca_key_secret_name"],
        secret_id=config["alpaca_key_secret_name"],
        project=config["project"],
        replication=secretmanager.SecretReplicationArgs(
            auto=secretmanager.SecretReplicationAutoArgs()
        )
    )

    alpaca_key_version = secretmanager.SecretVersion(
        f"{config['alpaca_key_secret_name']}-version",
        secret=alpaca_key_secret.id,
        secret_data=config["alpaca_api_key"]
    )

    alpaca_secret_secret = secretmanager.Secret(
        config["alpaca_secret_secret_name"],
        secret_id=config["alpaca_secret_secret_name"],
        project=config["project"],
        replication=secretmanager.SecretReplicationArgs(
            auto=secretmanager.SecretReplicationAutoArgs()
        )
    )

    alpaca_secret_version = secretmanager.SecretVersion(
        f"{config['alpaca_secret_secret_name']}-version",
        secret=alpaca_secret_secret.id,
        secret_data=config["alpaca_api_secret"]
    )

    # Create secret for Kafka credentials
    kafka_secret = secretmanager.Secret(
        config["kafka_secret_name"],
        secret_id=config["kafka_secret_name"],
        project=config["project"],
        replication=secretmanager.SecretReplicationArgs(
            auto=secretmanager.SecretReplicationAutoArgs()
        )
    )

    kafka_secret_version = secretmanager.SecretVersion(
        f"{config['kafka_secret_name']}-version",
        secret=kafka_secret.id,
        secret_data=kafka_details["api_key_secret"]
    )

    # Build and push the container image (GitHub-triggered build)
    image_name = f"gcr.io/{config['project']}/market-stream-consumer"

    cloudbuild_trigger = cloudbuild.Trigger(
        "market-stream-consumer-trigger",
        project=config["project"],
        filename="gcp-cloud-run-jobs/alpaca-realtime-stock-info-consumer/cloudbuild.yaml",
        location=config["region"],
        github=cloudbuild.TriggerGithubArgs(
            owner="dwessendorf",
            name="trading",
            push=cloudbuild.TriggerGithubPushArgs(branch="^main$")
        )
    )

    # Create Cloud Run v2 Job
    job = gcp.cloudrunv2.Job(
        config["job_name"],
        project=config["project"],
        location=config["region"],
        template=gcp.cloudrunv2.JobTemplateArgs(
            template=gcp.cloudrunv2.JobTemplateTemplateArgs(
                containers=[
                    gcp.cloudrunv2.JobTemplateTemplateContainerArgs(
                        image=image_name,
                        resources=gcp.cloudrunv2.JobTemplateTemplateContainerResourcesArgs(
                            limits={
                                "cpu": config["cpu"],
                                "memory": config["memory"],
                            },
                        ),
                        envs=[
                            gcp.cloudrunv2.JobTemplateTemplateContainerEnvArgs(
                                name="KAFKA_TOPIC_NAME",
                                value=config["kafka_topic_name"]
                            ),
                            gcp.cloudrunv2.JobTemplateTemplateContainerEnvArgs(
                                name="KAFKA_BOOTSTRAP_SERVER",
                                value=kafka_details["bootstrap_servers"]
                            ),
                            gcp.cloudrunv2.JobTemplateTemplateContainerEnvArgs(
                                name="KAFKA_API_KEY_ID",
                                value=kafka_details["api_key_id"]
                            ),
                            gcp.cloudrunv2.JobTemplateTemplateContainerEnvArgs(
                                name="KAFKA_SECRET_NAME",
                                value=config["kafka_secret_name"]
                            ),
                            gcp.cloudrunv2.JobTemplateTemplateContainerEnvArgs(
                                name="ALPACA_KEY_SECRET_NAME",
                                value=config["alpaca_key_secret_name"]
                            ),
                            gcp.cloudrunv2.JobTemplateTemplateContainerEnvArgs(
                                name="ALPACA_SECRET_SECRET_NAME",
                                value=config["alpaca_secret_secret_name"]
                            ),                    
                            gcp.cloudrunv2.JobTemplateTemplateContainerEnvArgs(
                                name="ALPACA_PAPER_TRADING",
                                value=config["alpaca_paper_trading"]
                            ),
                            gcp.cloudrunv2.JobTemplateTemplateContainerEnvArgs(
                                name="SYMBOLS",
                                value=config["stock_symbols"]
                            ),
                        ],
                    )
                ],
                max_retries=int(config["max_retries"]),
                timeout=f"{config['timeout_seconds']}s",
                service_account=service_account.email,
            )
        ),
    )

    # IAM Bindings (Vertex AI roles, Secret Manager, Logging)
    # Make sure to import pulumi_gcp.projects or reference them under gcp.projects:
    # from pulumi_gcp.projects import IAMBinding  (if you want direct usage)
    iam_ai_binding_1 = gcp.projects.IAMBinding(
        f"{job_name}-functionAIPermissions",
        members=[pulumi.Output.concat("serviceAccount:", service_account.email)],
        role="roles/aiplatform.user",
        project=config["project"],
        opts=pulumi.ResourceOptions(depends_on=[service_account])
    )

    iam_ai_binding_2 = gcp.projects.IAMBinding(
        f"{job_name}-functionAIPermissions2",
        members=[pulumi.Output.concat("serviceAccount:", service_account.email)],
        role="roles/aiplatform.modelUser",
        project=config["project"],
        opts=pulumi.ResourceOptions(depends_on=[service_account])
    )

    iam_secrets_binding = gcp.projects.IAMBinding(
        f"{job_name}-functionSecretPermissions",
        members=[pulumi.Output.concat("serviceAccount:", service_account.email)],
        role="roles/secretmanager.secretAccessor",
        project=config["project"],
        opts=pulumi.ResourceOptions(depends_on=[service_account])
    )

    iam_logging_binding = gcp.projects.IAMBinding(
        f"{job_name}-functionLoggingPermissions",
        members=[pulumi.Output.concat("serviceAccount:", service_account.email)],
        role="roles/logging.logWriter",
        project=config["project"],
        opts=pulumi.ResourceOptions(depends_on=[service_account])
    )

    # Create Cloud Scheduler job to invoke this Cloud Run Job
    scheduler_job = cloudscheduler.Job(
        f"{job_name}-scheduler",
        project=config["project"],
        region=config["region"],
        schedule=config["schedule"],
        time_zone="UTC",
        http_target=cloudscheduler.JobHttpTargetArgs(
            http_method="POST",
            uri=pulumi.Output.concat(
                "https://",
                config["region"],
                "-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/",
                config["project"],
                "/jobs/",
                config["job_name"],
                ":run"
            ),
            oauth_token=cloudscheduler.JobHttpTargetOauthTokenArgs(
                service_account_email=service_account.email,
            ),
        ),
    )

    return {
        "job_id": job.id,
        "job_name": job.name,
        "scheduler_job_id": scheduler_job.id,
    }
 