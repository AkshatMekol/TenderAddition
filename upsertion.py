import json
from tqdm import tqdm
from dateutil import parser
from datetime import datetime
from pymongo import UpdateOne
from config import STATE_URLS
from helpers import collection 
from config import JSONL_FILE, BATCH_SIZE

def parse_iso_date(date_str):
    if not date_str or str(date_str).strip() == "":
        return None
    try:
        dt = datetime.strptime(date_str, "%d-%b-%Y %I:%M %p")
        return dt
    except:
        try:
            dt = parser.parse(date_str)
            return dt.replace(tzinfo=None)
        except:
            return None

def enrich_tender_data(tender):
    work_item = tender.get("WorkItemDetails", {})
    basic_details = tender.get("BasicDetails", {})
    emd_details = tender.get("EmdFeeDetails", {})
    critical_dates = tender.get("CriticalDates", {})
    corrigendums = tender.get("Corrigenda") or []

    website = (tender.get("Website") or "").lower()
    organization_type = "Central"
    state_name = ""

    for state, url in STATE_URLS.items():
        if url.lower() in website:
            organization_type = "State"
            state_name = state
            break

    latest_corrigendum_date = None
    for c in corrigendums:
        details = c.get("Details") or []
        for d in details:
            dt = parse_iso_date(d.get("PublishedDate", ""))
            if dt and (latest_corrigendum_date is None or dt > latest_corrigendum_date):
                latest_corrigendum_date = dt

    def safe_float(val):
        try:
            return round(float(val or 0), 2)
        except:
            return 0.0

    enriched = {
        "unique_identifier": tender.get("UniqueIdentifier", ""),
        "updated_at": parse_iso_date(tender.get("UpdatedAt", "")),
        "website": tender.get("Website", ""),
        "link": tender.get("Link", ""),
        "description": work_item.get("Title", ""),
        "work_description": work_item.get("Description", ""),
        "tender_value": safe_float(work_item.get("TenderValue", 0)),
        "emd": safe_float(emd_details.get("EmdAmount", 0)),
        "published_date": parse_iso_date(critical_dates.get("PublishedDate", "")),
        "corrigendum_date": latest_corrigendum_date,
        "submission_date": parse_iso_date(critical_dates.get("BidSubmissionEndDate", "")),
        "organization": basic_details.get("OrganisationChain", "").split("||")[0],
        "organization_type": organization_type,
        "state": state_name,
        "organization_tender_id": basic_details.get("TenderID", ""),
        "type": basic_details.get("FormOfContract", ""),
        "category": basic_details.get("TenderCategory", ""),
        "product_category": work_item.get("ProductCategory", ""),
        "product_sub_category": work_item.get("SubCategory", ""),
        "completion_period": safe_float(work_item.get("PeriodOfWorkDays", 0)),
        "corrigendums": corrigendums
    }

    return enriched

def upsertion(file_path = JSONL_FILE, batch_size = BATCH_SIZE):
    batch_ops = []
    total_inserted = 0
    total_upserted = 0
    total_lines = 0

    with open(file_path, "r", encoding="utf-8") as f:
        for line in tqdm(f, desc="Reading JSONL"):
            total_lines += 1
            line = line.strip()
            if not line:
                continue
            try:
                tender = json.loads(line)
            except json.JSONDecodeError as e:
                print(f"⚠️ Skipping invalid JSON line {total_lines}: {e}")
                continue

            enriched = enrich_tender_data(tender)
            unique_id = enriched.get("unique_identifier")

            if unique_id:
                batch_ops.append(
                    UpdateOne(
                        {"unique_identifier": unique_id},
                        {"$set": enriched},
                        upsert=True
                    )
                )
            else:
                batch_ops.append(
                    UpdateOne(
                        {"_id": None},
                        {"$setOnInsert": enriched},
                        upsert=True
                    )
                )

            if len(batch_ops) >= batch_size:
                result = collection.bulk_write(batch_ops, ordered=False)
                total_inserted += result.upserted_count
                total_upserted += result.matched_count
                print(f"🧩 Processed batch of {len(batch_ops)} | Inserted: {total_inserted} | Upserted: {total_upserted}")
                batch_ops = []

    if batch_ops:
        result = collection.bulk_write(batch_ops, ordered=False)
        total_inserted += result.upserted_count
        total_upserted += result.matched_count
        print(f"🧩 Processed final batch of {len(batch_ops)} | Inserted: {total_inserted} | Upserted: {total_upserted}")

    print("\n✅ Upsert/Insert Completed!")
    print(f"📄 Total tenders in JSONL: {total_lines}")
    print(f"📊 Total inserted (new docs): {total_inserted}")
    print(f"📊 Total upserted/updated: {total_upserted}")
