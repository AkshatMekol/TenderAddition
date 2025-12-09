from bson import ObjectId
from datetime import datetime, timezone, date
from helpers import collection, profile_collection, score_collection, notification_collection

TODAY_STR = date.today().isoformat()

def get_high_compatibility_tenders(user_id):
    user = profile_collection.find_one({"user_id": user_id}, {"midpoint": 1})
    midpoint = user.get("midpoint", 500000000) if user else 500000000

    scores_cursor = score_collection.find({"user_id": user_id})
    scores_list = list(scores_cursor)
    if not scores_list:
        print(f"⚠️ No compatibility scores found for user {user_id}")
        return []

    tender_ids = [s["tender_id"] for s in scores_list]
    tenders_cursor = collection.find({"_id": {"$in": tender_ids}})
    tenders_map = {t["_id"]: t for t in tenders_cursor}

    low_value_scores = []
    high_value_scores = []

    for s in scores_list:
        tender_id = s["tender_id"]
        score = s.get("score", 0)
        tender = tenders_map.get(tender_id)
        if not tender:
            continue
        tender_value = tender.get("tender_value", 0)
        if tender_value < midpoint:
            low_value_scores.append((score, tender))
        else:
            high_value_scores.append((score, tender))

    low_value_scores.sort(key=lambda x: x[0], reverse=True)
    high_value_scores.sort(key=lambda x: x[0], reverse=True)

    low_threshold = low_value_scores[49][0] if len(low_value_scores) >= 50 else (low_value_scores[-1][0] if low_value_scores else 0)
    high_threshold = high_value_scores[9][0] if len(high_value_scores) >= 10 else (high_value_scores[-1][0] if high_value_scores else 0)

    print(f"User={user_id} | Low-value threshold (50th) = {low_threshold} | High-value threshold (10th) = {high_threshold}")

    top_low = low_value_scores[:50]
    top_high = high_value_scores[:10]

    notifications = []
    for score, tender in top_low + top_high:
        published = tender.get("published_date")
        if published and str(published)[:10] == TODAY_STR:
            notifications.append(("new_tender", tender))
            print(f"📌 Notify: Tender={tender['_id']} | Score={score} | Value={tender.get('tender_value')}")

    print(f"🎯 Total tenders to notify today: {len(notifications)}")
    return notifications

def get_changed_saved_tenders(user_id):
    user = profile_collection.find_one({"user_id": user_id})
    saved = user.get("saved_tenders", []) if user else []
    print(f"🔹 Total saved tenders: {len(saved)}")

    tender_ids = [ObjectId(item["id"]) for item in saved if item.get("id")]
    if not tender_ids:
        return []

    tenders_cursor = collection.find({"_id": {"$in": tender_ids}})
    changed_list = []

    for t in tenders_cursor:
        if t.get("corrigendum_date") and str(t["corrigendum_date"])[:10] == TODAY_STR:
            changed_list.append(t)
            print(f"📌 Saved tender updated today: {t['_id']}")

    print(f"🎯 Total saved tenders changed today: {len(changed_list)}")
    return changed_list

def build_notification(user_id, tender, ntype):
    if ntype == "new_tender":
        title = "Tender with high compatibility was found."
        content = tender.get('description', 'No description.')
    else:
        title = "Your Saved Tender has been updated."
        content = tender.get('description', 'No description.')

    print(f"💬 Building notification | Type: {ntype} | Tender ID: {tender['_id']}")
    return {
        "user_id": user_id,
        "tender_id": tender["_id"],
        "type": ntype,
        "message": {"title": title, "content": content},
        "created_at": datetime.now(timezone.utc),
        "seen": False
    }

def save_notifications(notifications):
    total = len(notifications)
    if total == 0:
        print("⚠️ No notifications to save.")
        return

    notification_collection.insert_many(notifications)
    print(f"✅ Saved {total} notifications!")

def notify():
    print("\n🚀 Running notification engine for ALL users...\n")
    profiles = profile_collection.find({}, {"user_id": 1})
    profiles = list(profiles)
    print(f"🟢 Total users found: {len(profiles)}\n")

    for idx, profile in enumerate(profiles, start=1):
        user_id = profile.get("user_id")
        print(f"\n==============================")
        print(f"👤 Processing User {idx}/{len(profiles)} | user_id={user_id}")
        print(f"==============================")
        try:
            high_comp = get_high_compatibility_tenders(user_id)
        except Exception as e:
            print(f"❌ Error in high compatibility check: {e}")
            continue
        try:
            changed = get_changed_saved_tenders(user_id)
        except Exception as e:
            print(f"❌ Error in changed saved tenders: {e}")
            continue

        notifications = []

        for (_, tender) in high_comp:
            notifications.append(build_notification(user_id, tender, "new_tender"))

        for tender in changed:
            notifications.append(build_notification(user_id, tender, "tender_updated"))

        print(f"📊 Notifications to save for user: {len(notifications)}")
        save_notifications(notifications)

    print("\n🎉 Completed notifications for ALL users!")
