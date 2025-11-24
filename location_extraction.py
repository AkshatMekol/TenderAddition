def capitalize_words(text):
    return " ".join(word.capitalize() for word in text.strip().split())

def prepare_prompt(tender):
    org = tender.get("organization", "")
    desc = tender.get("description", "")
    state_in_db = tender.get("state", "").strip()
    if state_in_db:
        prompt = PROMPT_WITH_STATE.format(organization=org, description=desc, state=state_in_db)
    else:
        prompt = PROMPT_NO_STATE.format(organization=org, description=desc)
    return prompt, state_in_db

def parse_deepseek_result(result_str, state_in_db):
    match = re.search(r'\{.*\}', result_str, re.DOTALL)
    if not match:
        return None, None

    json_str = match.group(0)
    try:
        res_dict = json.loads(json_str)
        city = res_dict.get("city", "unknown").strip()
        state = res_dict.get("state", state_in_db if state_in_db else "unknown").strip()
        return city, state
    except json.JSONDecodeError:
        return None, None

def normalize_location(city, state):
    if city.lower() == "unknown" and state.lower() == "unknown":
        return "", ""

    valid_state = None
    city = capitalize_words(city)
    state = capitalize_words(state)

    state = state.replace(" And ", " & ")
    normalized_input = state.replace("And", "&").replace(" ", "").lower()

    for st in STATE_CAPITALS:
        st_normalized = st.replace(" And ", " & ")
        st_normalized = st_normalized.replace("And", "&").replace(" ", "").lower()
        if st_normalized == normalized_input:
            valid_state = st
            break

    if not valid_state:
        return None, None

    state = valid_state

    if not city or city.lower() == "unknown":
        city = STATE_CAPITALS.get(state, "")

    return city, state

def update_mongo(tender_id, city, state):
    location = f"{city}, {state}" if city and state else ""
    try:
        collection.update_one(
            {"_id": tender_id},
            {"$set": {"city": city, "state": state, "location": location}}
        )
        return True
    except Exception:
        return False

def enrich_worker(tender):
    tender_id = tender.get("_id")
    prompt, state_in_db = prepare_prompt(tender)
    result_str = query_deepseek(prompt)

    if result_str in ("api_error", "", None):
        return "api_error"

    city, state = parse_deepseek_result(result_str, state_in_db)
    if city is None or state is None:
        return "format_error"

    city, state = normalize_location(city, state)
    if city is None or state is None:
        return "format_error"

    if not city and not state:
        return "unknown"

    if not update_mongo(tender_id, city, state):
        return "api_error"

    return None

def location_extraction():
    # tenders = list(collection.find({"location": {"$exists": False}}))
    tenders = list(collection.find({
        "location": {"$exists": False},
        "tender_value": {"$gt": 10000000}
    }))
    print(f"Found {len(tenders)} tenders to enrich (without existing location).")

    api_errors = 0
    format_errors = 0
    unknowns = 0

    with mp.Pool(processes=NUM_WORKERS_DEEPSEEK) as pool:
        results = list(tqdm(pool.imap_unordered(enrich_worker, tenders), total=len(tenders)))

    for r in results:
        if r == "api_error":
            api_errors += 1
        elif r == "format_error":
            format_errors += 1
        elif r == "unknown":
            unknowns += 1

    print("\n🎉 Enrichment complete!")
    print(f"⚠️ API errors: {api_errors}")
    print(f"⚠️ Format errors: {format_errors}")
    print(f"⚠️ Unknown responses: {unknowns}")
