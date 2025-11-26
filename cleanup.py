from helpers import collection, vector_collection, profiles_collection s3
from config import S3_BUCKET, S3_PREFIX
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

def get_valid_tender_ids():
    valid_ids = [str(_id) for _id in collection.distinct("_id")]
    return valid_ids

def get_valid_tender_ids():
    valid_ids = set(str(_id) for _id in collection.distinct("_id"))
    all_profiles = profiles_collection.find({}, {"my_tenders.id": 1})

    for profile in all_profiles:
        for tender in profile.get("my_tenders", []):
            valid_ids.add(tender["id"])

    return valid_ids

def get_total_distinct_tenderdocs_ids():
    distinct_ids = vector_collection.distinct("tender_id")
    print(f"📊 Total distinct tender_ids in TenderDocs: {len(distinct_ids)}")
    return distinct_ids

def get_orphan_tenderdocs(valid_ids):
    orphans = vector_collection.distinct(
        "tender_id",
        {"tender_id": {"$nin": list(valid_ids)}}
    )
    print(f"⚠️ Orphan TenderDocs Count: {len(orphans)}\n")
    return orphans

def delete_orphan_tenderdocs(orphans):
    total_deleted = 0
    for tid in tqdm(orphans, desc="Deleting orphan TenderDocs"):
        result = vector_collection.delete_many({"tender_id": tid})
        total_deleted += result.deleted_count
    print(f"\n✅ Deleted {total_deleted} documents from TenderDocs.")

def scan_s3_folders():
    folders = {}
    paginator = s3.get_paginator("list_objects_v2")
    for page in tqdm(paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_PREFIX)):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            parts = key.split("/")
            if len(parts) >= 2:
                folder_name = parts[1]
                if folder_name:
                    folders.setdefault(folder_name, 0)
                    folders[folder_name] += obj["Size"]
    print(f"\n📊 Total distinct tender_ids in S3: {len(folders)}")
    return folders

def get_orphan_s3_folders(folders, valid_ids):
    orphans_s3 = [f for f in folders if f not in valid_ids]
    total_size = sum(folders.values())
    orphan_size = sum(folders[f] for f in orphans_s3)
    print(f"⚠️ Orphan Folder Count: {len(orphans_s3)}")
    return orphans_s3

def delete_s3_folder(folder):
    prefix = f"{S3_PREFIX}{folder}/"
    paginator = s3.get_paginator("list_objects_v2")
    to_delete = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            to_delete.append({"Key": obj["Key"]})
            if len(to_delete) == 1000:
                s3.delete_objects(Bucket=S3_BUCKET, Delete={"Objects": to_delete})
                to_delete = []
    if to_delete:
        s3.delete_objects(Bucket=S3_BUCKET, Delete={"Objects": to_delete})

def delete_orphan_s3_folders(orphans_s3):
    print("\n🧹 Deleting orphan S3 folders in parallel...")
    with ThreadPoolExecutor(max_workers=16) as executor:
        list(tqdm(executor.map(delete_s3_folder, orphans_s3), total=len(orphans_s3)))
    print("\n✅ S3 Cleanup complete.")

def cleanup():
    valid_ids = get_valid_tender_ids()
    s3_folders = scan_s3_folders()
    orphan_s3 = get_orphan_s3_folders(s3_folders, valid_ids)
    print("\n")
    get_total_distinct_tenderdocs_ids()
    orphan_docs = get_orphan_tenderdocs(valid_ids)

    total_orphans = len(orphan_docs) + len(orphan_s3)
    if total_orphans == 0:
        print("\n✅ No orphan TenderDocs or S3 folders found. Nothing to delete.")
        return

    confirm = input(f"\nType 'yes' to delete {len(orphan_docs)} orphan TenderDocs and {len(orphan_s3)} orphan S3 folders: ").strip().lower()
    if confirm != "yes":
        print("\n❎ Deletion cancelled by user.")
        return

    if orphan_docs:
        delete_orphan_tenderdocs(orphan_docs)
    else:
        print("\n✅ No orphan TenderDocs to delete.")

    if orphan_s3:
        delete_orphan_s3_folders(orphan_s3)
    else:
        print("\n✅ No orphan S3 folders to delete.")
