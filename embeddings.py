import time
import openai
import pymongo
from pymongo import MongoClient
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed


# ----------------------------
# CONFIG
# ----------------------------





# ----------------------------
# OpenAI embedding with retry (no random)
# ----------------------------
def embed_batch(texts):
    for attempt in range(MAX_RETRIES):
        try:
            res = openai.embeddings.create(
                model=MODEL,
                input=texts
            )
            return [r.embedding for r in res.data]

        except Exception as e:
            wait = (2 ** attempt)
            print(f"⚠️ Error: {e} | retrying in {wait}s")
            time.sleep(wait)

    print("❌ Max retries reached for batch")
    return None


# ----------------------------
# Process one batch
# ----------------------------
def process_batch(col, batch_docs):
    texts = [(d.get("description") or "") for d in batch_docs]
    ids = [d["_id"] for d in batch_docs]

    vectors = embed_batch(texts)
    if not vectors:
        return 0

    ops = [
        pymongo.UpdateOne({"_id": _id}, {"$set": {"embeddings": vec}})
        for _id, vec in zip(ids, vectors)
    ]

    col.bulk_write(ops)
    return len(ops)


# ----------------------------
# MAIN
# ----------------------------
def run():
    client = MongoClient(MONGO_CONN_STRING)
    col = client[DB][COLL]

    docs = list(col.find(
        {"embeddings": {"$exists": False}},
        {"_id": 1, "description": 1}
    ))

    total = len(docs)
    if total == 0:
        print("✔ All embeddings already exist")
        return

    print(f"🔵 Need embeddings for {total} docs")

    batches = [docs[i:i+BATCH_SIZE] for i in range(0, total, BATCH_SIZE)]

    # Simple progress bar over batches
    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = [executor.submit(process_batch, col, batch) for batch in batches]

        for _ in tqdm(as_completed(futures), total=len(futures), desc="Embedding"):
            pass

    print("🎉 DONE - All embeddings generated!")


# ----------------------------
# RUN
# ----------------------------
run()
