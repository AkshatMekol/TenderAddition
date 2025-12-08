from pymongo import MongoClient
from bson import ObjectId
from datetime import datetime, timezone, date
from helpers import collection, profile_collection, score_collection, notification_collection

TODAY_STR = date.today().isoformat()

def get_high_compatibility_tenders(user_id, score_threshold=60):
    print(f"🔹 Fetching CompatibilityScores for user_id={user_id}")
    scores_cursor = score_collection.find({"user_id": user_id})
    
    scores_list = list(scores_cursor)
    print(f"📝 Total compatibility scores found: {len(scores_list)}")

    above_threshold = [s for s in scores_list if s.get("score", 0) >= score_threshold]
    print(f"✅ Scores above threshold ({score_threshold}): {len(above_threshold)}")

    if above_threshold:
        max_score = max(s.get("score", 0) for s in above_threshold)
        print(f"🏆 Highest score today: {max_score}")

    tenders = []
    today_count = 0
    for s in above_threshold:
        tender = collection.find_one({"_id": s["tender_id"]})
        if not tender:
            print(f"⚠️ Tender not found for tender_id={s['tender_id']}")
            continue

        published = tender.get("published_date")
        if published and str(published)[:10] == TODAY_STR:
            tenders.append(("new_tender", tender))
            today_count += 1
            print(f"📌 New tender today: {tender['_id']} | Score={s.get('score')}")

    print(f"🎯 Total high compatibility tenders published today: {today_count}")
    return tenders

def get_changed_saved_tenders(user_id):
    user = profile_collection.find_one({"user_id": user_id})
    saved = user.get("saved_tenders", []) if user else []
    print(f"🔹 Total saved tenders for user: {len(saved)}")

    changed_list = []
    changed_count = 0
    for item in saved:
        tid = item.get("id")
        if not tid:
            continue
        tender = collection.find_one({"_id": ObjectId(tid)})

        corrigendum = tender.get("corrigendum_date")
        if corrigendum and str(corrigendum)[:10] == TODAY_STR:
            changed_list.append(tender)
            changed_count += 1
            print(f"📌 Saved tender updated today: {tender['_id']}")

    print(f"🎯 Total saved tenders changed today: {changed_count}")
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
