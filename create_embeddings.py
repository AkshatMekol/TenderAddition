import time
import openai
import pymongo
from tqdm import tqdm
from pymongo import MongoClient
from concurrent.futures import ThreadPoolExecutor, as_completed
from config import BATCH_SIZE_EMBEDDINGS, OPENAI_API_KEY, WORKERS, MAX_RETRIES, MODEL, MONGO_URI, DB_NAME, TENDERS_COLLECTION

openai.api_key = OPENAI_API_KEY

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

def create_embeddings():
    client = MongoClient(MONGO_URI)
    col = client[DB_NAME][TENDERS_COLLECTION]

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
