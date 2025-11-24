s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)

client = MongoClient(MONGO_URI, tlsCAFile=certifi.where())
db = client[DB_NAME]
db_past = client[DB_NAME_PAST]
collection = db[TENDERS_COLLECTION]
status_collection = db[DOCS_STATUS_COLLECTION]
vector_collection = db[VECTOR_COLLECTION]
result_collection = db_past[RESULTS_COLLECTION]
competitor_collection = db_past[COMPETITORS_COLLECTION]
profile_collection = db[PROFILES_COLLECTION]
score_collection = db[SCORE_COLLECTION]
