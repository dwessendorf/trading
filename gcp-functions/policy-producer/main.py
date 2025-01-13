import logging
import os
import json
import random
import uuid
from datetime import datetime
import time
from faker import Faker
import datetime
from dateutil.relativedelta import relativedelta
from datetime import date
from decimal import Decimal
import re
from confluent_kafka import Producer
from google.cloud import secretmanager
from google.auth import default
from typing import Dict
import logging


########################################################################################
#  This python script creates synthetic data for car insurance policies. The data      #
#  contains artificial policy holder, car and tariff-details tuned for the german      # 
#  market. For the later creation of a churn prediction model it seeds some policy     # 
#  termination patterns into the data.                                                 #
########################################################################################

__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))

# Configure logging
logging.basicConfig(level=logging.INFO)


# Initialize Faker for the German market
fake = Faker(["de-DE"])

# Initialize input data arrays on global level
car_list = []
car_weight_list = []
city_list = []
city_weight_list = []

########################################################################################
# Fetch secrets from GCP Secret Manager
########################################################################################
def fetch_secret(secret_name: str, project_id: str) -> str:
    """Fetch a secret value from GCP Secret Manager."""
    client = secretmanager.SecretManagerServiceClient()
    secret_path = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    response = client.access_secret_version(name=secret_path)
    return response.payload.data.decode("UTF-8")

########################################################################################
# Kafka Producer Configuration
########################################################################################
def initialize_kafka_producer() -> Producer:
    """Initialize and return a Kafka producer."""
    # Fetch environment variables
    credentials, project_id = default()
    bootstrap_server = os.getenv("KAFKA_BOOTSTRAP_SERVER")
    kafka_topic_name = os.getenv("KAFKA_TOPIC_NAME")
    secret_name = os.getenv("SECRET_NAME")
    kafka_api_key_id = os.getenv("KAFKA_API_KEY_ID")
    

    # Fetch Kafka API secret from Secret Manager
    kafka_api_key_secret = fetch_secret(secret_name, project_id)

    kafka_config = {
        "bootstrap.servers": bootstrap_server,
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "sasl.username": kafka_api_key_id,
        "sasl.password": kafka_api_key_secret,
    }

    LOGGER.info(f"Kafka configuration initialized with topic: {kafka_topic_name}")
    return Producer(kafka_config), kafka_topic_name

########################################################################################
# Main Function: Produce Kafka Messages
########################################################################################
def generate_response(request):
    """Main function to produce Kafka records."""
    # Kafka initialization
    global car_list, car_weight_list, city_list, city_weight_list, region
    global LOGGER
    LOGGER = logging.getLogger(__name__)
    resp = {"status": False, "resp": ""}
    LOGGER.setLevel(logging.INFO)

    LOGGER.info(f"Initializing Kafka")
    producer, kafka_topic_name = initialize_kafka_producer()

    # Review generation configuration
    min_profiles = int(os.getenv("MIN_RECORDS_PER_RUN", "10"))
    max_profiles = int(os.getenv("MAX_RECORDS_PER_RUN", "20"))
    seconds_between_profiles = int(os.getenv("SECONDS_BETWEEN_PROFILES", "1"))
    LOGGER.info(f"Loading Data files")
    # Fill input data arrays during first lambda initialization
    if len(car_list) == 0: car_list = get_cars()
    if len(car_weight_list) == 0: car_weight_list = get_car_weights() 
    if len(city_list) == 0: city_list = get_cities() 
    if len(city_weight_list) == 0: city_weight_list = get_city_weights()


    record_count = 0
    LOGGER.info(f"Loading Data files")
    try:
        for _ in range(random.randint(min_profiles, max_profiles)):
            # Generate a profile

            profile = create_profile()
            print(profile)
            # Convert profile to JSON payload
            payload = json.dumps(profile)
    
            # Produce the record to Kafka
            producer.produce(kafka_topic_name, key=str(uuid.uuid4()), value=payload)
            LOGGER.info(f"Produced profile to Kafka: {payload}")
            record_count += 1

            # Simulate processing delay
            time.sleep(seconds_between_profiles)

    except Exception as e:
        LOGGER.error(f"Error producing profiles to Kafka: {e}")
    finally:
        producer.flush()
        return f"Produced {record_count} profiles to Kafka"

########################################################################################
#  Load city data from csv file into global array
########################################################################################
def get_cities():
    with open(os.path.join(__location__, './input_data/cities_zip_name_coords.csv'), 'r' ,encoding='utf-8-sig') as f:
        return  f.read().splitlines() 

########################################################################################
#  Load city weights from csv file into global array
########################################################################################
def get_city_weights(): 
    with open(os.path.join(__location__, './input_data/cities_cum_weights.csv'), 'r', encoding='utf-8-sig') as f:
        city_weight_list = f.read().splitlines() 
    return [float(x) for x in city_weight_list]

########################################################################################
#  Load car data from csv file into global array
########################################################################################
def get_cars(): 
    with open(os.path.join(__location__, './input_data/hsn_tsn_car_list.csv'), 'r', encoding='utf-8-sig' ) as f:
        return f.read().splitlines() 

########################################################################################
#  Load car weights from csv file into global array
########################################################################################
def get_car_weights():
    with open(os.path.join(__location__, './input_data/hsn_tsn_car_cum_weights.csv'), 'r', encoding='utf-8-sig') as f:
        car_weight_list = f.read().splitlines() 
    return [float(x) for x in car_weight_list]

########################################################################################
#  Create synthetic data profile (Customer data, address data, car data, policy data )
########################################################################################
def create_profile():

    profile_rec = {}
    profile = {}

    #Insured Person information
    profile_rec['UserID'] = str(uuid.uuid4())
    profile_rec['Sex'] = random.choices(population=["M", "F"], cum_weights=(493,1000)).pop()

    if profile_rec['Sex'] == "M":
        profile_rec["Prefix"] = "Herr"
        profile_rec["Title"] = fake.prefix_male().replace("Herr", "")
        profile_rec["FirstName"] = fake.first_name_male()
    else:
        profile_rec["Prefix"] = "Frau"
        profile_rec["Title"] = fake.prefix_female().replace("Frau", "")
        profile_rec["FirstName"] = fake.first_name_female()
    profile_rec["LastName"] = fake.last_name()  
    print("NormalizeName")
    profile_rec["NameSearchKey"] =  normalize_name(profile_rec["FirstName"], profile_rec["LastName"])




    actyear = datetime.date.today().year
    ph_birthday = datetime.date(random.randint(actyear-90, actyear-16), random.randint(1,12), random.randint(1,28))
    profile_rec['Birthdate'] =  ph_birthday.isoformat() 

    # Address Information
    print("InjectAddress")
    inject_address(profile_rec)
    
    # Car Information
    print("InjectCar")
    inject_car(profile_rec)

    # Insurance tariff information    
    # Tariff versions in t-shirt sizes and distribution using cumulative weights
    tariffs_list = ["XL", "L", "M", "S", "XS"]
    tariff_cum_weights_list = (10, 30, 70, 90, 100)
    tariff = random.choices(population=tariffs_list, cum_weights=tariff_cum_weights_list).pop()
    inject_tariff_details(profile_rec, tariff)   
 
    # Insurance policy information
    policy_age = max(random.normalvariate(300, 150),1)
    dt_policy_start = date.today() + relativedelta(days = -policy_age)
    profile_rec['CarPolicyState'] = 'Active'
    profile_rec['CarPolicyStartDate'] = dt_policy_start.isoformat()
    profile_rec['CarPolicyTerminationDate'] = ""

    # Who is allowed to drive the car?
    profile_rec['CarFamilyMembersIncluded'] = random.choices(population=["Yes", "No"], cum_weights=(60,100)).pop()
    age_policy_holder = relativedelta(date.today(), ph_birthday ).years
    if profile_rec['CarFamilyMembersIncluded'] == "Yes":
        if age_policy_holder > 16:
            profile_rec['CarAgeYoungestDriver'] = random.randrange(16,age_policy_holder)
        else:
            profile_rec['CarAgeYoungestDriver'] = 16
    else: 
        profile_rec['CarAgeYoungestDriver'] = age_policy_holder 


    # How many claims did the car had in the last months/years?
    profile_rec['CarNrOfClaimsLast24months'] = random.choices(population=["0", "1", "2", "3"], cum_weights=(80,95,99,100)).pop()

    if profile_rec['CarNrOfClaimsLast24months'] == "0":
        profile_rec['CarNoClaimsYears'] = int(round(random.gammavariate(6,1),0))
    else: 
        profile_rec['CarNoClaimsYears'] = random.randrange(0,1)
    
    # Where is the car stored?
    profile_rec['CarMainRepository'] = random.choices(population=["Garage", "Carport", "Street"], cum_weights=(30, 30, 40)).pop()

    # Decide on policystate incl. creation of termination patterns if applicable
    inject_policy_state(profile_rec)
    
    # Create quote
    inject_car_insurance_quote(profile_rec)

    return profile_rec


########################################################################################
#  Helper function to normalize names for index creation
########################################################################################
def normalize_name(firstname, lastname):
    # Mapping dictionary for german "Umlaute"
    mapping = {'ä': 'ae', 'ö': 'oe', 'ü': 'ue', 'ß': 'ss'}   
    # Concatenate the names with '_'
    name = f'{firstname}_{lastname}'
    # Replace German umlaute
    name = ''.join(mapping.get(c, c) for c in name)
    # Remove special characters
    name = re.sub(r'[^\w\s]', '', name)
    # Convert the string to lowercase
    name = name.lower()
    return name


########################################################################################
#  Determine policy state and termination reasons
########################################################################################
def inject_policy_state(profile_rec):
    
    policy_cancellation_decision = random.choices(population=["Active", "Terminated_Noise", "Terminated_Signal"], cum_weights=(75, 90, 100)).pop()
    if policy_cancellation_decision != "Active":
        profile_rec["CarPolicyState"] = "Terminated"
        policy_end_date = min(datetime.datetime.now() + relativedelta(days = -max(random.normalvariate(100, 50),1)), datetime.datetime.now() + relativedelta(days = -1 * int(random.randrange(1,20))))
        policy_age = max(random.normalvariate(330, 50),1)
        dt_policy_start = policy_end_date + relativedelta(days = -policy_age)
        profile_rec["CarPolicyStartDate"] = dt_policy_start.date().isoformat()
        profile_rec["CarPolicyTerminationDate"] = policy_end_date.date().isoformat()
        if policy_cancellation_decision == "Terminated_Signal":
            temination_pattern = random.choices(population=["LuxuryCarSmallTariffClaim", "NoLuxuryCarXLTariffNoClaim", "CarAgeYoungestDriverCityHighPremium"], cum_weights=(40, 80, 100)).pop()
            #temination_pattern = "LuxuryCarSmallTariffClaim"
            if temination_pattern == "LuxuryCarSmallTariffClaim":
                inject_luxury_car_small_tariff_claim_pattern(profile_rec)
            elif temination_pattern == "NoLuxuryCarXLTariffNoClaim":
                inject_no_luxury_car_big_tariff_no_claim_pattern(profile_rec)
            elif temination_pattern == "CarAgeYoungestDriverCityHighPremium":
                inject_car_age_youngest_driver_city_high_premium_pattern(profile_rec)

########################################################################################
#  Adapt profile for termination reason LuxuryCarSmallTariffClaim ( Customer is rather
#  underinsured and has a claim with low coverage and high cost on customer side)
########################################################################################
def inject_luxury_car_small_tariff_claim_pattern(profile_rec):
    while profile_rec['LuxuryCar'] == "no":
        inject_car(profile_rec)
    tariff = random.choices(population=["XS", "S"], cum_weights=(75, 100)).pop()
    inject_tariff_details(profile_rec, tariff)
    profile_rec['CarNrOfClaimsLast24months'] = random.choices(population=["1", "2", "3"], cum_weights=(80,95,100)).pop()
    profile_rec['CarNoClaimsYears'] = random.randrange(0,1)

########################################################################################
#  Adapt profile for termination reason NoLuxuryCarXLTariffNoClaim ( Customer is rather
#  overinsured and had no claims for longer time
########################################################################################    
def inject_no_luxury_car_big_tariff_no_claim_pattern(profile_rec):
    while profile_rec['LuxuryCar'] == "yes":
        inject_car(profile_rec)
    tariff = random.choices(population=["XL", "L"], cum_weights=(80, 100)).pop()
    inject_tariff_details(profile_rec, tariff)
    profile_rec['CarNrOfClaimsLast24months'] = "0"
    claims_free_policy_age = int(random.choices(population=["1", "2", "3", "4", "5" ], cum_weights=(20,60,80,90,100)).pop())
    profile_rec['CarNoClaimsYears'] = claims_free_policy_age
    policy_start_date = datetime.datetime.fromisoformat(profile_rec['CarPolicyStartDate']) - relativedelta(years=claims_free_policy_age)
    profile_rec['CarPolicyStartDate'] = policy_start_date.date().isoformat()

########################################################################################
#  Adapt profile for termination reason CarAgeYoungestDriverCityHighPremium (Customer is 
#  paying high premium due to age of youngest driver and metropolitan surcharge)
########################################################################################  
def inject_car_age_youngest_driver_city_high_premium_pattern(profile_rec):
    while float(profile_rec['PopulationDensityPerSqkm']) < 2000:
        inject_address(profile_rec)
    ph_birthday= datetime.datetime.fromisoformat(profile_rec['Birthdate'])
    age_policy_holder = relativedelta(date.today(), ph_birthday ).years
    profile_rec['CarFamilyMembersIncluded'] = "yes"
    if age_policy_holder > 16:
        profile_rec['CarAgeYoungestDriver'] = int(random.choices(population=["16", "17", "18", "19", "20" ], cum_weights=(75,87,95,98,100)).pop())
    else:
        profile_rec['CarAgeYoungestDriver'] = 16

########################################################################################
#  Randomly pick cars from input list and inject data into profile 
########################################################################################  
def inject_car(profile_rec):
    #Insured Object Car information
    car = random.choices(population=car_list, cum_weights=car_weight_list).pop()
    profile_rec['Hsn'] = car.split("|")[0]
    profile_rec['Make'] = car.split("|")[1]
    profile_rec['Tsn'] = car.split("|")[2]
    profile_rec['Model'] = car.split("|")[3]
    profile_rec['Year'] =  min(date.today().year, date.today().year - int(round(random.gammavariate(6,2),0)))    # random.randint(1980, 2022)
    profile_rec['LuxuryCar'] =  get_luxury_flag(profile_rec['Make'])         

########################################################################################
#  Randomly pick cities from input list and inject data into profile 
########################################################################################  
def inject_address(profile_rec):
    print("getCity")

    city = random.choices(population=city_list, cum_weights=city_weight_list).pop()
    print(city)
    profile_rec['Street'] = fake.street_name()
    profile_rec['HouseNumber'] = fake.building_number()
    profile_rec['ZipCode'] = city.split("|")[0]
    profile_rec['City'] = city.split("|")[1]
    profile_rec['Longitude'] = city.split("|")[2]
    profile_rec['Latitude'] = city.split("|")[3]
    profile_rec['PopulationDensityClass'] = city.split("|")[4]
    profile_rec['PopulationDensityPerSqkm'] = city.split("|")[5]


########################################################################################
#  Simple logic for luxury car indicator
######################################################################################## 
def get_luxury_flag(make):
    luxury_flag = "no"
    if "vw" in make.lower(): luxury_flag = "yes"
    if "bmw" in make.lower(): luxury_flag = "yes"
    if "daimler" in make.lower(): luxury_flag = "yes"
    if "audi" in make.lower(): luxury_flag = "yes"
    if "mercedes" in make.lower(): luxury_flag = "yes"
    if "volvo" in make.lower(): luxury_flag = "yes"
    if "kia" in make.lower(): luxury_flag = "yes"
    if "porsche" in make.lower(): luxury_flag = "yes"
    if "toyota" in make.lower(): luxury_flag = "yes"
    if "tesla" in make.lower(): luxury_flag = "yes"
    if "land rover" in make.lower(): luxury_flag = "yes"
    if "jaguar" in make.lower(): luxury_flag = "yes"
    if "quattro" in make.lower(): luxury_flag = "yes"
    if "ferrari" in make.lower(): luxury_flag = "yes"
    if "maserati" in make.lower(): luxury_flag = "yes"
    if "alpina" in make.lower(): luxury_flag = "yes"
    if "mg rover" in make.lower(): luxury_flag = "yes"
    if "bentley" in make.lower(): luxury_flag = "yes"
    if "rolls-royce" in make.lower(): luxury_flag = "yes"
    if "renault-rvi" in make.lower(): luxury_flag = "yes"
    if "lamborghini" in make.lower(): luxury_flag = "yes"    
    return luxury_flag


########################################################################################
#  Inject tariff details into profile
######################################################################################## 
def inject_tariff_details(profile_rec, tariff):

    if tariff == "XL":
        profile_rec["CarTariffName"] = "CarProtect XL"
        profile_rec["CarAnnualPremium"] = "600"
        profile_rec["CarCoverThirdParty"] = "yes"
        profile_rec["CarCoverFireAndTheft"] = "yes"
        profile_rec["CarCoverComprehensive"] = "yes"
        profile_rec["CarCoverFullyComprehensive"] = "yes"
        profile_rec["CarCoverBreakdown"] = "yes"
        profile_rec["CarCoverActOfGod"] = "yes"
        profile_rec["CarExcessThirdParty"] = "0"
        profile_rec["CarExcessFireAndTheft"] = "0"
        profile_rec["CarExcessComprehensive"] = "0"
        profile_rec["CarExcessFullyComprehensive"] = "0"

    elif tariff == "L":
        profile_rec["CarTariffName"] = "CarProtect L"
        profile_rec["CarAnnualPremium"] = "400"
        profile_rec["CarCoverThirdParty"] = "yes"
        profile_rec["CarCoverFireAndTheft"] = "yes"
        profile_rec["CarCoverComprehensive"] = "yes"
        profile_rec["CarCoverFullyComprehensive"] = "no"
        profile_rec["CarCoverBreakdown"] = "yes"
        profile_rec["CarCoverActOfGod"] = "yes"
        profile_rec["CarExcessThirdParty"] = "150"
        profile_rec["CarExcessFireAndTheft"] = "150"
        profile_rec["CarExcessComprehensive"] = "1500"
        profile_rec["CarExcessFullyComprehensive"] = "1500"

    elif tariff == "M":
        profile_rec["CarTariffName"] = "CarProtect M"
        profile_rec["CarAnnualPremium"] = "300"
        profile_rec["CarCoverThirdParty"] = "yes"
        profile_rec["CarCoverFireAndTheft"] = "yes"
        profile_rec["CarCoverComprehensive"] = "yes"
        profile_rec["CarCoverFullyComprehensive"] = "no"
        profile_rec["CarCoverBreakdown"] = "no"
        profile_rec["CarCoverActOfGod"] = "no"
        profile_rec["CarExcessThirdParty"] = "300"
        profile_rec["CarExcessFireAndTheft"] = "300"
        profile_rec["CarExcessComprehensive"] = "300"
        profile_rec["CarExcessFullyComprehensive"] = "0"

    elif tariff == "S": 
        profile_rec["CarTariffName"] = "CarProtect S"
        profile_rec["CarAnnualPremium"] = "200"
        profile_rec["CarCoverThirdParty"] = "yes"
        profile_rec["CarCoverFireAndTheft"] = "yes"
        profile_rec["CarCoverComprehensive"] = "no"
        profile_rec["CarCoverFullyComprehensive"] = "no"
        profile_rec["CarCoverBreakdown"] = "no"
        profile_rec["CarCoverActOfGod"] = "no"
        profile_rec["CarExcessThirdParty"] = "600"
        profile_rec["CarExcessFireAndTheft"] = "600"
        profile_rec["CarExcessComprehensive"] = "600"
        profile_rec["CarExcessFullyComprehensive"] = "0"

    elif tariff == "XS":
        profile_rec["CarTariffName"] = "CarProtect XS"
        profile_rec["CarAnnualPremium"] = "150"
        profile_rec["CarCoverThirdParty"] = "yes"
        profile_rec["CarCoverFireAndTheft"] = "no"
        profile_rec["CarCoverComprehensive"] = "no"
        profile_rec["CarCoverFullyComprehensive"] = "no"
        profile_rec["CarCoverBreakdown"] = "no"
        profile_rec["CarCoverActOfGod"] = "no"
        profile_rec["CarExcessThirdParty"] = "600"
        profile_rec["CarExcessFireAndTheft"] = "0"
        profile_rec["CarExcessComprehensive"] = "0"
        profile_rec["CarExcessFullyComprehensive"] = "0"


########################################################################################
#  Run quotation logic and inject annual premium into profile
######################################################################################## 
def inject_car_insurance_quote(profile_rec) :
    
    # Initialize quote with base premium from Tariff 
    quote = float(profile_rec["CarAnnualPremium"])

    # Population Density Factor
    if profile_rec['PopulationDensityClass'] == "03":
        quote = quote * 0.9 #  low density lowers premium
    elif profile_rec['PopulationDensityClass'] == "02":
        quote = quote * 1 #  mid-size density keeps base premium
    elif profile_rec['PopulationDensityClass'] == "01":
        if float(profile_rec['PopulationDensityPerSqkm']) < 2000: # bigger cities with moderate density slightly raise premium
            quote = quote * 1.1
        else:
            quote = quote * 1.2 # big cities with high density significantly raise premium

    # Luxury Car Factor
    if profile_rec['LuxuryCar']  == "yes": 
        quote = quote * 1.15
    # TBD

    # Family Member Surcharge
    if profile_rec['CarFamilyMembersIncluded'] == "yes":
        quote = quote * 1.1

    # Age youngest driver factor
    if profile_rec['CarAgeYoungestDriver'] < 18:
        quote = quote * 1.3
    elif profile_rec['CarAgeYoungestDriver'] < 21:
        quote = quote * 1.2
    elif profile_rec['CarAgeYoungestDriver'] < 25:
        quote = quote * 1.1
    elif profile_rec['CarAgeYoungestDriver'] < 30:
        quote = quote * 1.05
    elif profile_rec['CarAgeYoungestDriver'] > 85:
        quote = quote * 1.2
    elif profile_rec['CarAgeYoungestDriver'] > 75:
        quote = quote * 1.1
    elif profile_rec['CarAgeYoungestDriver'] > 60:
        quote = quote * 1.05

    # No claims discount
    if profile_rec['CarNoClaimsYears'] >= 12:
        quote = quote * 0.5
    elif profile_rec['CarNoClaimsYears'] == 0:
        quote = quote * 1.2
    elif profile_rec['CarNoClaimsYears'] == 1:
        quote = quote * 1.1
    elif profile_rec['CarNoClaimsYears'] == 2:
        quote = quote * 1.05
    elif profile_rec['CarNoClaimsYears'] == 3:
        quote = quote * 0.95
    elif profile_rec['CarNoClaimsYears'] == 4:
        quote = quote * 0.9     
    elif profile_rec['CarNoClaimsYears'] == 5:
        quote = quote * 0.85
    elif profile_rec['CarNoClaimsYears'] == 6:
        quote = quote * 0.8
    elif profile_rec['CarNoClaimsYears'] == 7:
        quote = quote * 0.75
    elif profile_rec['CarNoClaimsYears'] == 8:
        quote = quote * 0.7
    elif profile_rec['CarNoClaimsYears'] == 9:
        quote = quote * 0.65
    elif profile_rec['CarNoClaimsYears'] == 10:
        quote = quote * 0.6
    elif profile_rec['CarNoClaimsYears'] == 11:
        quote = quote * 0.55

    # Main repository factor
    if profile_rec['CarMainRepository'] == "Garage":
        quote = quote * 0.95
    elif profile_rec['CarMainRepository'] == "Street":
        quote = quote * 1.1        

    quote = round(quote, 4)
    profile_rec["CarAnnualPremium"] = quote


########################################################################################
#  Helper function for serialization of data to dynamodb
######################################################################################## 
def convert_float_to_decimal(s): 
    return Decimal(str(round(float(s), 2)))


########################################################################################
# Helper function for local testing
########################################################################################
if __name__ == "__main__":
    os.environ["KAFKA_BOOTSTRAP_SERVER"] = "SASL_SSL://pkc-7xoy1.eu-central-1.aws.confluent.cloud:9092"
    os.environ["KAFKA_TOPIC_NAME"] = "customer-profiles-topic"
    os.environ["SECRET_NAME"] = "aws-confluent-key"
    os.environ["KAFKA_API_KEY_ID"] = "EY2OIEQ2YFFIXE6C"
    os.environ["PROJECT_ID"] = "partner-engineering"
    os.environ["MIN_PROFILES"] = "5"
    os.environ["MAX_PROFILES"] = "10"
    os.environ["SECONDS_BETWEEN_PROFILES"] = "2"

    generate_response(None)