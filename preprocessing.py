from helpers import collection, parse_date_naive
from config import JSONL_FILE
import json
from tqdm import tqdm
from dateutil import parser

def get_latest_updated_at(collection):
    latest_doc = collection.find_one(
        {"updated_at": {"$ne": None}},
        sort=[("updated_at", -1)],
        projection={"updated_at": 1}
    )

    if not latest_doc:
        print("⚠️ No updated_at found in Mongo — treating all as fresh.")
        return [], []

    latest_mongo_updated = latest_doc["updated_at"]
    print(f"\n🕒 Latest updated_at in Mongo: {latest_mongo_updated}")
    return latest_mongo_updated

def find_closed_tenders(jsonl_file):
    jsonl_ids = set()
    with open(jsonl_file, "r", encoding="utf-8") as f:
        for line in tqdm(f, desc="Reading JSONL"):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                uid = obj.get("UniqueIdentifier")
                if uid:
                    jsonl_ids.add(uid)
            except json.JSONDecodeError:
                continue

    print(f"\n📦 Found {len(jsonl_ids)} unique IDs in JSONL")

    mongo_ids = set(
        d["unique_identifier"]
        for d in collection.find({"unique_identifier": {"$ne": None}}, {"unique_identifier": 1})
    )

    closed = list(mongo_ids - jsonl_ids)
    print(f"🗂️ Found {len(closed)} closed tenders (in Mongo but not in JSONL)")
    return closed

def find_stale_tenders(jsonl_file):
    latest_mongo_updated = get_latest_updated_at(collection)

    fresh, stale = [], []

    with open(jsonl_file, "r", encoding="utf-8") as f:
        for line in tqdm(f, desc="Scanning JSONL for freshness"):
            line = line.strip()
            if not line:
                continue
            try:
                tender = json.loads(line)
            except json.JSONDecodeError:
                continue

            published_str = tender.get("CriticalDates", {}).get("PublishedDate")
            published = parse_date_naive(published_str)

            latest_corr = None
            for corr in tender.get("Corrigenda") or []:
                for d in corr.get("Details") or []:
                    dt_str = d.get("PublishedDate")
                    dt = parse_date_naive(dt_str)
                    if dt and (latest_corr is None or dt > latest_corr):
                        latest_corr = dt

            most_recent = max([d for d in [published, latest_corr] if d is not None], default=None)

            if most_recent and most_recent > latest_mongo_updated:
                fresh.append(tender)
            else:
                stale.append(tender)

    print(f"🆕 Fresh tenders: {len(fresh)}")
    print(f"📉 Stale tenders: {len(stale)}")
    return fresh, stale

def preprocessing():
    print("============== DRY RUN ==============")
    closed = find_closed_tenders(JSONL_FILE)
    fresh, stale = find_stale_tenders(JSONL_FILE)

    print("\n============== SUMMARY ==============")
    print(f"🔸 Closed tenders to delete from Mongo: {len(closed)}")
    print(f"🔸 Stale tenders to delete from JSONL: {len(stale)}")

    confirm = input("\n❓ Proceed with deletion? (yes/no): ").strip().lower()
    if confirm != "yes":
        print("❎ Dry run complete — no deletions made.")
        return

    if closed:
        result = collection.delete_many({"unique_identifier": {"$in": closed}})
        print(f"🗑️ Deleted {result.deleted_count} closed tenders from Mongo.")
    else:
        print("✅ No closed tenders to delete.")

    # output_file = JSONL_FILE
    # with open(output_file, "w", encoding="utf-8") as fout:
    #     for tender in fresh:
    #         json.dump(tender, fout, ensure_ascii=False)
    #         fout.write("\n")
    # print(f"🧹 JSONL cleaned — {len(stale)} stale tenders delete")

    print("\n🎯 Cleanup completed successfully.")
