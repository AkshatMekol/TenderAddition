import time
from tqdm import tqdm
from bson import ObjectId
from pymongo import MongoClient, UpdateOne
from config import VECTOR_INDEX_NAME, TOP_K
from helpers import collection, profile_collection, score_collection

def vector_search(query_vec, top_k=TOP_K):
    pipeline = [
        {
            "$vectorSearch": {
                "index": VECTOR_INDEX_NAME,
                "path": "embeddings",
                "queryVector": query_vec,
                "numCandidates": top_k * 2,
                "limit": top_k,
            }
        },
        {
            "$project": {
                "_id": 1,                     
                "score": {"$meta": "vectorSearchScore"}
            }
        }
    ]

    start = time.time()
    results = list(collection.aggregate(pipeline))
    print(f"⏱ Vector search: {time.time() - start:.2f}s — {len(results)} results")
    return results
    
def add_similarity_scores_for_all_users():
    profiles_cursor = profile_collection.find({}, {"saved_tenders": 1, "user_id": 1, "company_name": 1})
    profiles = list(profiles_cursor)
    print(f"🟢 Found {len(profiles)} profiles.")

    for profile_idx, profile in enumerate(tqdm(profiles, desc="Processing profiles"), start=1):
        profile_name = profile.get("company_name", "<unknown>")
        saved_ids = profile.get("saved_tenders", [])
        user_id = profile.get("user_id")

        print(f"\n📌 Processing profile {profile_idx}/{len(profiles)}: {profile_name}")

        if not saved_ids:
            print(f"⚠ No saved tenders for user '{profile_name}'. Skipping...")
            continue

        if isinstance(user_id, str):
            try:
                user_id = ObjectId(user_id)
            except Exception as e:
                print(f"⚠ Error converting user_id to ObjectId: {e}")

        user_tender_max = {} 

        for idx, tid in enumerate(saved_ids, start=1):
            print(f"   🔹 Processing saved tender {idx}/{len(saved_ids)}: {tid}")
            tender = collection.find_one({"_id": ObjectId(tid)}, {"embeddings": 1, "description": 1})
            if not tender:
                print(f"   ⚠ Tender ID {tid} not found. Skipping...")
                continue
            if "embeddings" not in tender:
                print(f"   ⚠ Tender ID {tid} has no embeddings. Skipping...")
                continue

            query_vec = tender["embeddings"]
            print(f"      Description preview: {tender.get('description', '')[:80]}...")

            try:
                similar_tenders = vector_search(query_vec, top_k=TOP_K)
            except Exception as e:
                print(f"   ⚠ Error during vector search for tender {tid}: {e}")
                continue

            print(f"      Found {len(similar_tenders)} similar tenders.")

            for sim in similar_tenders:
                tender_id = sim["_id"]
                additional_score = round(sim["score"] * 10, 2)
                if tender_id not in user_tender_max or additional_score > user_tender_max[tender_id]:
                    user_tender_max[tender_id] = additional_score

        if user_tender_max:
            print(f"   🟢 Applying scores to {len(user_tender_max)} tenders for user '{profile_name}'...")
            try:
                batch_ops = [
                    UpdateOne(
                        {"tender_id": tid, "user_id": user_id},
                        {"$inc": {"score": score}},
                        upsert=True
                    )
                    for tid, score in user_tender_max.items()
                ]
                score_collection.bulk_write(batch_ops, ordered=False)
                print(f"   ✅ Scores applied successfully for user '{profile_name}'")
            except Exception as e:
                print(f"   ⚠ Error during bulk write for user '{profile_name}': {e}")
        else:
            print(f"   ⚠ No similarity scores to apply for user '{profile_name}'")
            
    print(f"\n🎉 All similarity scores applied for all profiles in {round(end - start, 2)} seconds.")
