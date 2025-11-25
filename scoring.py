import json
from tqdm import tqdm
from bson import ObjectId
from pymongo import InsertOne
from collections import Counter
from config import BATCH_SIZE
from helpers import (
    collection,
    db_past,
    competitor_collection,
    profile_collection,
    score_collection,
    haversine
)

def get_tenders_matching_keywords(keywords):
    if not keywords:
        return set()

    pipeline = [
        {
            "$search": {
                "index": "TenderSearch",
                "compound": {
                    "should": [
                        {
                            "phrase": {
                                "query": kw,
                                "path": [
                                    "work_description",
                                    "description",
                                    "organization",
                                    "product_category",
                                    "product_sub_category"
                                ]
                            }
                        }
                        for kw in keywords
                    ]
                }
            }
        },
        {"$project": {"_id": 1}}
    ]

    cursor = collection.aggregate(pipeline)
    ids = {doc["_id"] for doc in cursor}

    return ids

def calculate_participation_score(company_name):
    competitor = competitor_collection.find_one(
        {"name": company_name},
        {"participated_tenders": 1}
    )

    if not competitor or "participated_tenders" not in competitor:
        return {}

    tender_ids = competitor["participated_tenders"]
    results_cursor = db_past["Results"].find(
        {"_id": {"$in": tender_ids}},
        {"organization": 1, "website": 1}
    )

    org_counter, web_counter = Counter(), Counter()
    for r in results_cursor:
        if org := r.get("organization"):
            org_counter.update([str(org).strip()])
        if web := r.get("website"):
            web_counter.update([str(web).strip()])

    total_orgs = sum(org_counter.values()) or 1
    total_webs = sum(web_counter.values()) or 1

    participation_scores = {}
    for web, count in web_counter.items():
        participation_scores[web] = (count / total_orgs) * 5
    for org, count in org_counter.items():
        base = (count / total_orgs) * 10
        participation_scores[org] = base + 5

    return participation_scores

def score_big_tender(company_info, tender_info, participation_score):
    def tender_amount_fit():
        tender_amt = tender_info.get("tender_value")
        min_amt, max_amt = company_info.get("preferred_tender_amount_range", (0, 0))
        midpoint = (min_amt + max_amt) / 2
        if midpoint == min_amt or tender_amt is None or min_amt >= max_amt:
            return 0
        if tender_amt <= min_amt or tender_amt >= max_amt:
            return 0
        elif tender_amt <= midpoint:
            return ((tender_amt - min_amt) / (midpoint - min_amt)) * 30
        else:
            return ((max_amt - tender_amt) / (max_amt - midpoint)) * 30

    def proximity():
        tender_coords = tender_info.get("coordinates")
        if not tender_coords:
            return 15
        sites = company_info.get("hq_locations", []) + company_info.get("regional_offices", []) + company_info.get("ongoing_sites", [])
        max_weighted_score = 0
        for site in sites:
            coords = site.get("coordinates")
            factor = site.get("factor", 1)
            if not coords:
                continue
            dist_km = haversine(tender_coords, coords)
            if 0 <= dist_km <= 50:
                score = 25
            elif 50 < dist_km <= 250:
                score = 25 - ((dist_km - 50) / 200) * 10
            elif 250 < dist_km <= 500:
                score = 15 - ((dist_km - 250) / 250) * 5
            else:
                score = 5
            weighted_score = score * factor
            max_weighted_score = max(max_weighted_score, weighted_score)
        return max_weighted_score

    total_score = round(tender_amount_fit(), 2) + round(proximity(), 2) + participation_score + 10
    return round(total_score, 1)


def score_small_tender(company_info, tender_info, participation_score):
    def tender_amount_fit():
        tender_amt = tender_info.get("tender_value")
        midpoint = company_info.get("midpoint") or 500000000
        if tender_amt is None:
            return 0
        return (tender_amt / midpoint) * 10

    def proximity():
        tender_coords = tender_info.get("coordinates")
        if not tender_coords:
            return 0
        sites = company_info.get("hq_locations", []) + company_info.get("regional_offices", []) + company_info.get("ongoing_sites", [])
        max_weighted_score = 0
        for site in sites:
            coords = site.get("coordinates")
            factor = site.get("factor", 1)
            if not coords:
                continue
            dist_km = haversine(tender_coords, coords)
            if dist_km <= 55:
                score = 55 - ((dist_km / 50) * 10)
            elif dist_km <= 200:
                score = 45 - ((dist_km - 50) / 150 * (45 - 10))
            else:
                score = max(0, 10 - ((dist_km - 200) / 800) * 10)
            weighted_score = score * factor
            max_weighted_score = max(max_weighted_score, weighted_score)
        return max_weighted_score

    total_score = round(tender_amount_fit(), 2) + round(proximity(), 2) + participation_score
    return round(total_score, 1)

def submit_for_scoring():
    score_collection.drop()
    print("✅ CompatibilityScores collection dropped.")
    profiles = list(profile_collection.find({}, {"company_info": 1, "company_name": 1, "user_id": 1}))
    tenders_cursor = collection.find({}, {"tender_value": 1, "coordinates": 1, "organization": 1, "website": 1, "organization_type": 1})

    tenders = []
    for t in tenders_cursor:
        tenders.append({
            "_id": t["_id"],
            "tender_value": t.get("tender_value"),
            "coordinates": t.get("coordinates"),
            "organization": t.get("organization"),
            "website": t.get("website"),
            "organization_type": t.get("organization_type", "State")
        })
    print(f"🚀 Preprocessed {len(tenders)} tenders once.")

    batch_size = BATCH_SIZE
    ops = []

    for profile in tqdm(profiles, desc="Scoring profiles"):
        company_info = profile.get("company_info")
        if not company_info:
            continue

        company_name = profile.get("company_name")
        participation_scores = calculate_participation_score(company_name)
        matching_tender_ids = get_tenders_matching_keywords(company_info.get("keywords", []))

        midpoint = profile.get("midpoint") or 500000000

        for tender in tenders:
            tender_value = tender.get("tender_value")
            if tender_value is None:
                continue

            participation_score_big = (
                participation_scores.get(tender["organization"], 0)
                + participation_scores.get(tender["website"], 0)
            )

            participation_score_small = (
                participation_scores.get(tender["organization"], 0) / 3
                + participation_scores.get(tender["website"], 0) / 2
            )

            tender_info = {
                "tender_value": tender_value,
                "coordinates": tender.get("coordinates"),
                "organization_type": tender.get("organization_type", "State")
            }

            if tender_value >= midpoint:
                score = score_big_tender(company_info, tender_info, participation_score_big)
            else:
                score = score_small_tender(company_info, tender_info, participation_score_small)

            if tender["_id"] in matching_tender_ids:
                score = min(score + 10, 100)

            user_id = profile["user_id"]
            if isinstance(user_id, str):
                try:
                    user_id = ObjectId(user_id)
                except:
                    pass

            ops.append(InsertOne({
                "tender_id": tender["_id"],
                "user_id": user_id,
                "score": score
            }))

            if len(ops) >= batch_size:
                score_collection.bulk_write(ops, ordered=False)
                ops.clear()

    if ops:
        score_collection.bulk_write(ops, ordered=False)

    print("✅ Scoring completed and stored successfully.")
    score_collection.create_index([("user_id", 1), ("score", -1)])
    score_collection.create_index([("tender_id", 1), ("user_id", 1)], unique=True)
    print("✅ Indexes created successfully.")
