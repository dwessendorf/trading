import logging
import os
import yaml
import json
import uuid
import random
import time
from datetime import datetime
from confluent_kafka import Producer, KafkaError
from google.cloud import aiplatform
from google.auth import default
import vertexai
from vertexai.generative_models import (
    GenerationConfig,
    GenerativeModel,
)
from google.cloud import secretmanager
from typing import Dict, Optional
# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("customer-review-generator")

def load_config_from_secret(secret_name: str, project_id: str) -> Dict:
    """Load configuration from a secret in GCP Secret Manager."""
    # logger.info(f"Fetching configuration from secret: {secret_name}")
    try:
        client = secretmanager.SecretManagerServiceClient(client_options={"api_endpoint": "europe-west3-secretmanager.googleapis.com:443"})
        secret_path = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
        response = client.access_secret_version(name=secret_path)
        secret_payload = response.payload.data.decode("UTF-8")
        return yaml.safe_load(secret_payload)
    except secretmanager.SecretManagerServiceException as e:
        # logger.critical(f"Failed to fetch configuration from Secret Manager: {e}")
        raise
    except Exception as e:
        # logger.exception("Unexpected error while loading configuration")
        raise

class VertexAIClient:
    """A client for interacting with Google Cloud's Vertex AI Gemini models."""

    def __init__(
        self,
        project_id: str,
        location: str = "europe-west3",
        model_id: str = "gemini-1.5-flash-002",
    ):
        self.project_id = project_id
        self.location = location
        self.model_id = model_id

        aiplatform.init(project=project_id)
        vertexai.init(project=project_id, location=location)

        self.model = GenerativeModel(model_id)

    def _generate_dynamic_config(self) -> GenerationConfig:
        return GenerationConfig(
            temperature=random.uniform(0.7, 2.0),
            top_p=random.uniform(0.5, 1.0),
            top_k=random.randint(20, 40),
            candidate_count=random.randint(1, 3),
            max_output_tokens=random.randint(256, 768),
        )

    def generate_content(
        self,
        prompt: str,
        custom_generation_config: Optional[GenerationConfig] = None,
    ) -> Dict:
        try:
            contents = [prompt]
            generation_config = custom_generation_config or self._generate_dynamic_config()
            response = self.model.generate_content(
                contents,
                generation_config=generation_config,
            )
            candidates = response.candidates
            result_texts = [candidate.text for candidate in candidates]

            return {
                "texts": result_texts,
                "used_config": generation_config.to_dict(),
                "usage_metadata": response.to_dict().get("usage_metadata"),
                "finish_reason": candidates[0].finish_reason,
                "safety_ratings": candidates[0].safety_ratings,
            }
        except Exception as e:
            logger.exception(f"Error generating content with Vertex AI: {e}")
            raise

def retry_with_backoff(func, max_retries=3, initial_delay=1, backoff_factor=2):
    delay = initial_delay
    for attempt in range(max_retries):
        try:
            return func()
        except Exception as e:
            # logger.warning(f"Retry {attempt + 1}/{max_retries} failed: {e}")
            time.sleep(delay)
            delay *= backoff_factor
    # logger.error(f"Operation failed after {max_retries} retries")
    raise

def generate_persona() -> Dict:
    """
    Generate a random persona based on age, profession, and cultural background.
    
    Returns:
        A dictionary containing the persona's details.
    """
    ages = range(18, 101)
    professions = [
        "Teacher", "Software Developer", "Nurse", "Chef", "Artist", "Engineer", "Lawyer",
        "Doctor", "Carpenter", "Actor", "Writer", "Farmer", "Musician", "Scientist",
        "Accountant", "Mechanic", "Police Officer", "Salesperson", "Designer", "Athlete",
        "Barista", "Pilot", "Librarian", "Photographer", "Social Worker", "Entrepreneur",
        "Journalist", "Electrician", "Dentist", "Translator"
    ]
    cultural_backgrounds = [
        "American", "Chinese", "Indian", "French", "Mexican", "Italian", "Japanese",
        "Brazilian", "German", "Nigerian", "Russian", "South African", "Korean",
        "Australian", "British", "Egyptian", "Canadian", "Spanish", "Turkish",
        "Indigenous", "Middle Eastern", "Caribbean", "Southeast Asian", "Scandinavian",
        "Pacific Islander", "Eastern European", "Latin American", "Sub-Saharan African"
    ]

    persona = {
        "age": random.choice(ages),
        "profession": random.choice(professions),
        "cultural_background": random.choice(cultural_backgrounds),
    }
    return persona
def adapt_prompt_with_persona(scenario: str, sentiment: str, mood: str) -> str:
    """
    Adapt the prompt to include a persona's details and adjust the style accordingly.
    
    Args:
        scenario: The scenario for the review.
        sentiment: The sentiment of the review ("positive" or "negative").
        mood: The mood associated with the sentiment.
        
    Returns:
        A detailed prompt string including persona details.
    """
    persona = generate_persona()
    persona_description = (
        f"{persona['age']}-year-old {persona['profession']} "
        f"with a {persona['cultural_background']} background"
    )

    # Adapt the tone and style based on persona
    tone_instructions = (
        "Use casual and youthful language with some slang." if persona["age"] < 30 else
        "Use professional and articulate language." if persona["age"] < 60 else
        "Use reflective and mature language, perhaps nostalgic."
    )

    prompt = f"""
        User input:
        Embody a persona of a {persona_description}. 
        Scenario: {scenario}
        Sentiment: {mood}
        Style: {tone_instructions}
        Create a vivid and creative review for an insurance company called Insurancia. 
        Include minor typos and varied writing styles to reflect the persona. 
        Length: 30-70 words.
        Answer:
    """
    return prompt

def fetch_secret(secret_name: str, project_id: str) -> str:
    """Fetch a secret value from GCP Secret Manager."""
    client = secretmanager.SecretManagerServiceClient()
    secret_path = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    print(secret_path)
    response = client.access_secret_version(name=secret_path)
    return response.payload.data.decode("UTF-8")


def generate_response(request):
    
    credentials, project_id = default()
    logger.info("Initializing the Cloud Function")

    #Fetch configurations from environment variables
    kafka_topic_name = os.getenv("KAFKA_TOPIC_NAME")
    bootstrap_server = os.getenv("KAFKA_BOOTSTRAP_SERVER")
    secret_name = os.getenv("SECRET_NAME")
    kafka_api_key_id = os.getenv("KAFKA_API_KEY_ID")
    min_reviews_per_run = int(os.getenv("MIN_RECORDS_PER_RUN", "10"))
    max_reviews_per_run = int(os.getenv("MAX_RECORDS_PER_RUN", "20"))
    
    seconds_between_reviews  = int(os.getenv("SECONDS_BETWEEN_REVIEWS"))

    # Fetch Kafka API key secret from Secret Manager
    kafka_api_key_secret = fetch_secret(secret_name, project_id)

    kafka_config = {
        'bootstrap.servers': bootstrap_server,
        'security.protocol': "SASL_SSL",
        'sasl.mechanism': "PLAIN",
        'sasl.username': kafka_api_key_id,
        'sasl.password': kafka_api_key_secret
    }
    print(kafka_config)
    producer = Producer(kafka_config)
    vertex_client = VertexAIClient(project_id=project_id)

    # Load scenarios
    positive_scenarios = open("positive_scenarios.csv").read().splitlines()
    negative_scenarios = open("negative_scenarios.csv").read().splitlines()

    # Main review generation logic
    record_count = 0

    try:
        for _ in range(random.randint(min_reviews_per_run, max_reviews_per_run)):
            negative_moods = [
                "Frustration", "Irritation", "Resentment", "Worry", "Anxiety", "Dread",
                "Disappointment", "Hopelessness", "Loneliness", "Contempt", "Aversion", "Rejection"
            ]
            positive_moods = [
                "Cheerfulness", "Excitement", "Delight", "Contentment",
                "Appreciation", "Thankfulness", "Warmth", "Hopefulness",
                "Confidence", "Positivity", "Affection", "Care", "Compassion"
            ]

            sentiment = random.choices(["positive", "negative"], cum_weights=[80, 100])[0]
            mood = random.choice(positive_moods if sentiment == "positive" else negative_moods)
            scenario = random.choice(positive_scenarios if sentiment == "positive" else negative_scenarios)

            # Adapted prompt with persona
            prompt = adapt_prompt_with_persona(scenario, sentiment, mood)

            try:
                response = vertex_client.generate_content(prompt)
                review = random.choice(response["texts"]).strip()
                # logger.info(f"Generated Review: {review}")
            except Exception as e:
                # logger.error("Skipping due to content generation error")
                continue

            payload = json.dumps({
                "Name": f"Random User {uuid.uuid4()}",
                "Review": review,
                "CustomerReviewTimestamp": datetime.now().isoformat()
            })

            try:
                retry_with_backoff(lambda: producer.produce(kafka_topic_name, key=str(uuid.uuid4()), value=payload))
                record_count += 1
            except Exception as e:
                # logger.error("Failed to send to Kafka after retries")
                continue

            time.sleep(seconds_between_reviews)

    except Exception as e:
        logger.exception("Unexpected error in the review generation loop")
    finally:
        producer.flush()
        #return "Successful"
        return f"Produced {record_count} records"
        

if __name__ == "__main__":
    try:
        generate_response(None)
    except Exception as e:

        logger.critical("Unhandled exception in main execution", exc_info=True)