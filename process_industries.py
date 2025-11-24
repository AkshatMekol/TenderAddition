from tqdm import tqdm
from pymongo import UpdateOne
from helpers import collection     
from config import CATEGORY_TO_INDUSTRIES, BATCH_SIZE 

def map_industries(pc: str):
    if not pc:
        return None
    key = pc.strip()
    return CATEGORY_TO_INDUSTRIES.get(key)

def process_industries():
    total = collection.count_documents({"industries": {"$exists": False}})
    print(f"🔎 Total documents to process (no industries yet): {total}")

    ops = []
    processed = 0
    updated = 0

    cursor = collection.find(
        {"industries": {"$exists": False}},
        {"product_category": 1}
    )

    for doc in tqdm(cursor, total=total, desc="Updating"):
        industries = map_industries(doc.get("product_category", ""))

        if industries is not None:
            ops.append(UpdateOne(
                {"_id": doc["_id"]},
                {"$set": {"industries": industries}}
            ))
            updated += 1

        processed += 1

        if len(ops) >= BATCH_SIZE:
            collection.bulk_write(ops)
            ops.clear()
            print(f"✅ Processed {processed} docs... Updated so far: {updated}")

    if ops:
        collection.bulk_write(ops)

    print(f"🎉 Completed. Total processed: {processed}, Total updated: {updated}")

    query = {"$or": [{"industries": {"$exists": False}}, {"industries": {"$size": 0}}]}
    product_categories = collection.distinct("product_category", query)
    print(f"\nTotal unique product categories (without industries): {len(product_categories)}")
    for cat in product_categories:
        print(cat)
