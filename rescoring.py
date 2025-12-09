import time
from tqdm import tqdm
from bson import ObjectId
from pymongo import UpdateOne
from config import VECTOR_INDEX_NAME, TOP_K
from helpers import profile_collection, score_collection, embedding_collection

def vector_search(query_vec, top_k=TOP_K):
    pipeline = [
        {
            "$vectorSearch": {
                "index": VECTOR_INDEX_NAME,
                "path": "embedding",      
                "queryVector": query_vec,
                "numCandidates": top_k * 2,
                "limit": top_k,
            }
        },
        {
            "$project": {
                "tender_id": 1,
                "score": {"$meta": "vectorSearchScore"}
            }
        }
    ]

    start = time.time()
    results = list(embedding_collection.aggregate(pipeline))
    print(f"⏱ Vector search: {time.time() - start:.2f}s — {len(results)} results")
    return results

def rescore():
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

        for idx, item in enumerate(saved_ids, start=1):
            tid = item.get("id")  # stringified tender ObjectId
            print(f"   🔹 Processing saved tender {idx}/{len(saved_ids)}: {tid}")

            if not tid:
                print("   ⚠ Invalid saved_tender entry, missing 'id'. Skipping...")
                continue

            emb_doc = embedding_col.find_one({"tender_id": tid}, {"embedding": 1})
            if not emb_doc or "embedding" not in emb_doc:
                print(f"   ⚠ Tender ID {tid} has no embedding. Skipping...")
                continue

            query_vec = emb_doc["embedding"]

            try:
                similar_tenders = vector_search_fast(query_vec, top_k=TOP_K)
            except Exception as e:
                print(f"   ⚠ Error during vector search for tender {tid}: {e}")
                continue

            print(f"      Found {len(similar_tenders)} similar tenders.")

            for sim in similar_tenders:
                tender_id = ObjectId(sim["tender_id"])  
                additional_score = round(sim["score"] * 10, 2)

                if tender_id not in user_tender_max or additional_score > user_tender_max[tender_id]:
                    user_tender_max[tender_id] = additional_score

        if user_tender_max:
            print(f"   🟢 Applying scores to {len(user_tender_max)} tenders for user '{profile_name}'...")
            try:
                batch_ops = [
                    UpdateOne(
                        {"tender_id": tid, "user_id": user_id},
                        [
                            {"$set": {
                                "score": {
                                    "$round": [
                                        {"$min": [{"$add": [{"$ifNull": ["$score", 0]}, score]}, 100]},
                                        2
                                    ]
                                }
                            }}
                        ],
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
