import time
import openai
import pymongo
from tqdm import tqdm
from pymongo UpdateOne
from concurrent.futures import ThreadPoolExecutor, as_completed
from helpers import collection
from config import MODEL, WORKERS, MAX_RETRIES, BATCH_SIZE_EMBEDDINGS, OPENAI_API_KEY

openai.api_key = OPENAI_API_KEY

def embed_batch(texts):
    for attempt in range(MAX_RETRIES):
        try:
            res = openai.embeddings.create(
                model=MODEL,
                input=texts
            )
            return [d.embedding for d in res.data]

        except Exception as e:
            wait = 2 ** attempt
            print(f"⚠️ Error: {e} | retrying in {wait:.1f}s")
            time.sleep(wait)

    print("❌ Max retries reached for batch")
    return None

def process_batch(col_src, col_emb, batch_docs):
    texts = [(d.get("description") or "") for d in batch_docs]
    ids = [str(d["_id"]) for d in batch_docs]  

    vectors = embed_batch(texts)
    if vectors is None:
        return 0

    ops = []
    for tender_id, vec in zip(ids, vectors):
        ops.append(
            UpdateOne(
                {"tender_id": tender_id},
                {"$set": {"tender_id": tender_id, "embedding": vec}},
                upsert=True
            )
        )

    col_emb.bulk_write(ops)
    return len(ops)

def create_embeddings():
    print("🔍 Fetching docs missing embeddings...")

    existing = set(
        d["tender_id"]
        for d in embedding_collection.find({}, {"tender_id": 1})
    )

    docs = list(
        collection.find(
            {"_id": {"$nin": [ObjectId(x) for x in existing]}},
            {"_id": 1, "description": 1}
        )
    )

    total = len(docs)
    if total == 0:
        print("✔ All embeddings already exist")
        return

    print(f"🔵 Need embeddings for {total} docs")

    batches = [docs[i:i + BATCH_SIZE] for i in range(0, total, BATCH_SIZE)]

    pbar = tqdm(total=total, desc="Embedding")

    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = {
            executor.submit(process_batch, collection, embedding_collection, batch): batch
            for batch in batches
        }

        for future in as_completed(futures):
            done = future.result()
            pbar.update(done)

    pbar.close()
    print("🎉 DONE — All embeddings stored in TenderEmbeddings with stringified tender_id!")
