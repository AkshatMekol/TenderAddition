from preporcessing import preprocessing.py
from cleanup import cleanup
from upsertion import upsertion
from process_industries import process_industries
from process_locations import process_locations
from process_coordinates import prepare_locations, process_coordinates
from scoring import submit_for_scoring

if __name__ == "__main__":

    print("\n\n===== STEP 1: Preprocessing JSONL & Mongo =====")
    preprocessing()

    print("\n\n===== STEP 2: Cleaning up S3 and Vectors =====")
    cleanup()

    print("\n\n===== STEP 3: Upsert / Enrich Tenders =====")
    upsertion()

    print("\n\n===== STEP 4: Map Product Categories to Industries =====")
    process_industries()

    print("\n\n===== STEP 5: Enrich Tenders with City/State (Deepseek) =====")
    process_locations()

    print("\n\n===== STEP 6: Geocode Coordinates for Tenders =====")
    all_missing, existing_coords, to_geocode = prepare_locations()
    process_coordinates(all_missing, existing_coords, to_geocode)

    print("\n\n===== STEP 7: Score Tenders for Company Profiles =====")
    submit_for_scoring()

    print("\n\n🎯 All steps completed successfully!")
