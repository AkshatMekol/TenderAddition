from bson import ObjectId
from datetime import datetime, timezone, date
from helpers import collection, profile_collection, score_collection, notification_collection

TODAY_STR = date.today().isoformat()

def get_high_compatibility_tenders(user_id, score_threshold=60):
    scores = list(score_collection.find({"user_id": user_id, "score": {"$gte": score_threshold}}))
    print(f"✅ Scores above threshold ({score_threshold}): {len(scores)}")
    if not scores:
        return []

    tender_ids = [s["tender_id"] for s in scores]
    tenders_cursor = collection.find({"_id": {"$in": tender_ids}})
    tenders_map = {t["_id"]: t for t in tenders_cursor}

    today_tenders = []
    for s in scores:
        t = tenders_map.get(s["tender_id"])
        if not t:
            print(f"⚠️ Tender not found: {s['tender_id']}")
            continue
        if t.get("published_date") and str(t["published_date"])[:10] == TODAY_STR:
            today_tenders.append(("new_tender", t))
            print(f"📌 New tender today: {t['_id']} | Score={s['score']}")

    print(f"🎯 Total high compatibility tenders today: {len(today_tenders)}")
    return today_tenders

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
