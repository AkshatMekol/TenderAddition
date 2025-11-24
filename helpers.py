from pymongo import ASCENDING, DESCENDING
import re
import json
import certifi
import requests
import time
import uuid
import math
import boto3
import pandas as pd
import multiprocessing as mp
from bson import ObjectId
from math import radians, sin, cos, sqrt, atan2
from pymongo import MongoClient, UpdateOne, UpdateMany, InsertOne
from datetime import datetime, timezone
from tqdm import tqdm
from dateutil import parser, tz
from concurrent.futures import ThreadPoolExecutor
from collections import Counter

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

def parse_date_naive(date_str):
    if not date_str or str(date_str).strip() == "":
        return None
    try:
        dt = datetime.strptime(date_str, "%d-%b-%Y %I:%M %p")
        return dt
    except:
        try:
            dt = parser.parse(date_str)
            return dt.replace(tzinfo=None)
        except:
            return None

def query_deepseek(prompt, MODEL_NAME="deepseek-chat", retries=2, backoff=2):
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {DEEPSEEK_API_KEY}"}
    payload = {
        "model": MODEL_NAME,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.3
    }

    for attempt in range(1, retries + 1):
        try:
            response = requests.post(DEEPSEEK_API_URL, headers=headers, json=payload, timeout=30)
            if response.status_code == 200:
                data = response.json()
                choices = data.get("choices", [])
                if choices:
                    content = choices[0].get("message", {}).get("content", "").strip()
                    if content:
                        return content
                return '{"city": "unknown"}'
            elif response.status_code >= 500 or response.status_code == 429:
                time.sleep(backoff * attempt)
                continue
            else:
                return 'api_error'
        except requests.exceptions.RequestException:
            time.sleep(backoff * attempt)
    return 'api_error'

def geocode_address(address):
    headers = {
        "X-Request-Id": str(uuid.uuid4()),
        "X-Correlation-Id": str(uuid.uuid4())
    }
    params = {
        "address": address,
        "language": "English",
        "api_key": OLA_API_KEY
    }
    try:
        response = requests.get(OLA_MAPS_BASE_URL, headers=headers, params=params, timeout=15)
        data = response.json()
        results = data.get("geocodingResults", [])
        if results:
            loc = results[0].get("geometry", {}).get("location", {})
            lat, lng = loc.get("lat"), loc.get("lng")
            if lat is not None and lng is not None:
                return [lat, lng]
    except Exception as e:
        print(f"❌ Error geocoding {address}: {e}")
    return []

def haversine(coord1, coord2):
    if not coord1 or not coord2:
        return float("inf")
    lat1, lon1 = coord1
    lat2, lon2 = coord2
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    return R * (2 * math.atan2(math.sqrt(a), math.sqrt(1 - a)))
