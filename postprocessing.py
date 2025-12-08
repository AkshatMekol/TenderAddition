from pymongo import UpdateOne
from datetime import datetime
from config import BATCH_SIZE
from helpers import collection

def postprocessing():
    today = datetime.now()
    tonight = today.replace(hour=23, minute=59, second=0, microsecond=0)
    print(f"📅 Current datetime: {today}")
    print(f"🌙 Future dates will be normalized to tonight: {tonight}\n")

    cursor = collection.find({})
    total_docs = cursor.collection.count_documents({})
    print(f"🔍 Found {total_docs} tenders in total.\n")

    batch_ops = []
    count = 0
    updated_pub = 0
    updated_corr = 0

    for doc in cursor:
        doc_id = doc["_id"]

        pub_date = doc.get("published_date")
        if pub_date and pub_date > today:
            pub_date = tonight
            updated_pub += 1

        corr_date = doc.get("corrigendum_date")
        if corr_date and corr_date > today:
            corr_date = tonight
            updated_corr += 1

        updated_at = max(d for d in [pub_date, corr_date] if d is not None)

        batch_ops.append(
            UpdateOne(
                {"_id": doc_id},
                {"$set": {
                    "published_date": pub_date,
                    "corrigendum_date": corr_date,
                    "updated_at": updated_at
                }}
            )
        )

        count += 1

        if len(batch_ops) >= BATCH_SIZE:
            result = collection.bulk_write(batch_ops, ordered=False)
            print(f"🧩 Processed batch of {len(batch_ops)} | Matched: {result.matched_count} | Modified: {result.modified_count}")
            batch_ops = []

    if batch_ops:
        result = collection.bulk_write(batch_ops, ordered=False)
        print(f"🧩 Processed final batch of {len(batch_ops)} | Matched: {result.matched_count} | Modified: {result.modified_count}")

    print("\n🎯 Normalization complete!")
    print(f"Total tenders processed: {count}")
    print(f"Published dates updated: {updated_pub}")
    print(f"Corrigendum dates updated: {updated_corr}")
