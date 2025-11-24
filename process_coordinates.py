def worker(location):
    coords = geocode_address(location)
    return location, coords

def prepare_locations():
    print("\n===== Preparing Locations for Geocoding =====\n")
    query_missing = {"$or": [{"coordinates": {"$exists": False}}, {"coordinates": {"$size": 0}}]}
    all_missing_locations = collection.distinct("location", query_missing)

    existing_coords = {}
    cursor = collection.aggregate([
        {"$match": {"coordinates": {"$exists": True, "$ne": []}}},
        {"$group": {"_id": "$location", "coordinates": {"$first": "$coordinates"}}}
    ])
    for doc in cursor:
        loc = doc["_id"]
        coords = doc.get("coordinates")
        existing_coords[loc] = coords

    to_geocode = []
    for loc in all_missing_locations:
        if loc not in existing_coords:
            to_geocode.append(loc)
        else:
            continue

    print(f"Total locations with missing coordinates: {len(all_missing_locations)}")
    print(f"Locations to fetch from geocode API: {len(to_geocode)}")
    print(f"Cached locations that will be used to fill missing docs: {len(existing_coords)}\n")

    return all_missing_locations, existing_coords, to_geocode

def process_coordinates(all_missing, existing_coords, to_geocode):
    print("\n===== Processing Locations =====\n")
    not_found = set()

    if to_geocode:
        with mp.Pool(NUM_WORKERS_OLA) as pool:
            for loc, coords in tqdm(pool.imap_unordered(worker, to_geocode), total=len(to_geocode)):
                if coords:
                    existing_coords[loc] = coords
                else:
                    not_found.add(loc)
                    print(f"❌ Could not geocode: {loc}")

    bulk_ops = []
    count = 0
    for loc in all_missing:
        coords = existing_coords.get(loc, [])

        bulk_ops.append(
            UpdateMany(
                {"location": loc, "$or": [{"coordinates": {"$exists": False}}, {"coordinates": {"$size": 0}}]},
                {"$set": {"coordinates": coords}}
            )
        )

        if len(bulk_ops) >= BATCH_SIZE:
            result = collection.bulk_write(bulk_ops)
            batch_updated = sum([op.modified_count for op in result.bulk_api_result['writeErrors']]) if 'writeErrors' in result.bulk_api_result else result.bulk_api_result['nModified']
            count += batch_updated
            bulk_ops.clear()

    if bulk_ops:
        result = collection.bulk_write(bulk_ops)
        batch_updated = result.bulk_api_result['nModified']
        count += batch_updated

    print(f"🎉 Geocoding and updates completed.")
    print(f"✅ Total documents updated with coordinates: {count}")
    print(f"⚠️ Locations that remain without coordinates (API failed): {len(not_found)}")
    if not_found:
        print(f"❌ Locations not found: {list(not_found)}\n")


if __name__ == "__main__":
    all_missing, existing_coords, to_geocode = prepare_locations()
    process_locations(all_missing, existing_coords, to_geocode)
