from pulumi_gcp import storage, secretmanager, cloudfunctions, serviceaccount, projects, cloudscheduler
import pulumi


def deploy_cloud_function_with_secrets(config, source_dir, kafka_details):
    """Deploy a Cloud Function with Kafka configuration and service account."""

    function_name = config["function_name"]
    function_name_18= function_name[:18]

    # Step 1: Create a Service Account
    service_account = serviceaccount.Account(
            f"{function_name_18}-sa",
            display_name=f"{function_name}-gcp-function-serviceaccount",
    )


    # Step 2: Create a GCP Secret for the Kafka API Key Secret
    kafka_secret = secretmanager.Secret(
        f"{function_name}-kafka-secret-basic",
        secret_id=config["kafka_secret_name"],
        replication={
            "user_managed": {
                "replicas": [
                    {
                        "location": "europe-west3",
                    },
                ],
            },
        })

    # Add the Kafka API key secret as a version
    kafka_secret_version = secretmanager.SecretVersion(
        f"{function_name}-kafka-secret-version",
        secret=kafka_secret.id,
        secret_data=kafka_details["api_key_secret"],
    )

    # Step 3: Create a GCP Secret for the Alphavantage API Key Secret
    alpha_vantage_secret = secretmanager.Secret(
        f"{function_name}-alva-secret-basic",
        secret_id=config["alpha_vantage_secret_name"],
        replication={
            "user_managed": {
                "replicas": [
                    {
                        "location": "europe-west3",
                    },
                ],
            },
        })

    # Add the Kafka API key secret as a version
    alpha_vantage_secret_version = secretmanager.SecretVersion(
        f"{function_name}-alva-secret-version",
        secret=alpha_vantage_secret.id,
        secret_data=config["alpha_vantage_secret_value"],
    )

    # Step 4: Create a Cloud Storage Bucket for Function Source Code
    bucket = storage.Bucket(
        f"{function_name}-source-code-bucket",
        location=config["region"],
        force_destroy=True,
    )

    # Upload the function source code to the bucket
    bucket_object = storage.BucketObject(
       f"{function_name}-source-code",
        bucket=bucket.name,
        source=pulumi.FileArchive(source_dir),
    )

    # Step 5: Deploy the Cloud Function with the Service Account
    function = cloudfunctions.Function(
        function_name,
        name=function_name,
        runtime=config["runtime"],
        region=config["region"],
        available_memory_mb=512,
        timeout=530,
        max_instances=5,
        entry_point=config["entry_point"],
        source_archive_bucket=bucket.name,
        source_archive_object=bucket_object.name,
        trigger_http=True,
        service_account_email=service_account.email,  # Attach the service account
        environment_variables={
            "KAFKA_API_KEY_ID": kafka_details["api_key_id"],  # Plaintext API Key ID
            "KAFKA_SECRET_NAME": config["kafka_secret_name"],            # Kafka Secret
            "ALPHA_VANTAGE_SECRET_NAME": config["alpha_vantage_secret_name"],            # Kafka Secret
            "PROJECT_ID": config["project"],            # GCP Secret
            "KAFKA_BOOTSTRAP_SERVER": kafka_details["bootstrap_servers"],
            "KAFKA_TOPIC_NAME": config["kafka_topic_name"],
            "MIN_RECORDS_PER_RUN": config["min_reviews_per_run"],
            "MAX_RECORDS_PER_RUN": config["max_reviews_per_run"],
            "SECONDS_BETWEEN_REVIEWS": config["seconds_between_reviews"],
        },
        opts=pulumi.ResourceOptions(depends_on=[service_account])
    )

    # After creating the function
    function_invoker = cloudfunctions.FunctionIamMember(
       f"{function_name}-function-invoker",
        project=config.get("project"),
        region=config["region"],
        cloud_function=function.name,
        role="roles/cloudfunctions.invoker",
        member=pulumi.Output.concat("serviceAccount:", service_account.email),
        opts=pulumi.ResourceOptions(depends_on=[service_account, function])
    )

    # Step 6: Assign Vertex AI Role to Service Account
    iam_ai_binding = projects.IAMBinding(
        f"{function_name}-functionAIPermissions",
        members=[pulumi.Output.concat("serviceAccount:", service_account.email)],
        role="roles/aiplatform.user",
        project=config["project"],
        opts=pulumi.ResourceOptions(depends_on=[service_account])  # Ensure project_id is correctly passed
    )

        # Step 6: Assign Vertex AI Role to Service Account
    iam_ai_binding = projects.IAMBinding(
        f"{function_name}-functionAIPermissions2",
        members=[pulumi.Output.concat("serviceAccount:", service_account.email)],
        role="roles/aiplatform.modelUser",
        project=config["project"],
        opts=pulumi.ResourceOptions(depends_on=[service_account])  # Ensure project_id is correctly passed
    )

    iam_secrets_binding = projects.IAMBinding(
        f"{function_name}-functionSecretPermissions",
        members=[pulumi.Output.concat("serviceAccount:", service_account.email)],
        role="roles/secretmanager.secretAccessor",
        project=config["project"],
        opts=pulumi.ResourceOptions(depends_on=[service_account])  # Ensure project_id is correct ly passed
    )

    iam_logging_binding = projects.IAMBinding(
        f"{function_name}-functionLoggingPermissions",
        members=[pulumi.Output.concat("serviceAccount:", service_account.email)],
        role="roles/logging.logWriter",
        project=config["project"],
        opts=pulumi.ResourceOptions(depends_on=[service_account])  # Ensure project_id is correctly passed
    )


    job = cloudscheduler.Job(
        f"{function_name}-scheduler-job",
        name=f"{function_name}-scheduler-job",
        description=f"{function_name}-scheduler-job",
        schedule="* * * * *",
        time_zone="Europe/Berlin",
        attempt_deadline="320s",
        region=config["region"],
        http_target={
            "http_method": "POST",
            "uri": function.https_trigger_url,
            "oidc_token": {
                "service_account_email": service_account.email,
            },
        })



    # Export function URL and secret details
    pulumi.export(f"{function_name}_function_url", function.https_trigger_url)
    pulumi.export(f"{function_name}_service_account_email", service_account.email)
    pulumi.export(f"{function_name}_kafka_api_secret_name", kafka_secret.secret_id)
