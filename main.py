from preprocessing import preprocessing
from cleanup import cleanup
from upsertion import upsertion
from process_industries import process_industries
from process_locations import process_locations
from process_coordinates import prepare_locations, process_coordinates
from create_embeddings import create_embeddings
from scoring import submit_for_scoring
from rescoring import rescore
from notifier import notify
from postprocessing import postprocessing

if __name__ == "__main__":

    print("\n\n==================== STEP 1: Preprocessing JSONL & Mongo ====================")
    preprocessing()

    print("\n\n\n==================== STEP 2: Cleaning up S3 and Vectors ====================")
    cleanup()

    print("\n\n\n==================== STEP 3: Upsert / Enrich Tenders ====================")
    upsertion()

    print("\n\n\n==================== STEP 4: Map Product Categories to Industries ====================")
    process_industries()

    print("\n\n\n==================== STEP 5: Enrich Tenders with City/State (Deepseek) ====================")
    process_locations()

    print("\n\n\n==================== STEP 6: Geocode Coordinates for Tenders ====================")
    all_missing, existing_coords, to_geocode = prepare_locations()
    process_coordinates(all_missing, existing_coords, to_geocode)

    print("\n\n\n==================== STEP 7: Create Embeddings for Tenders ====================")
    create_embeddings()

    print("\n\n\n==================== STEP 8: Score Tenders for Company Profiles ====================")
    submit_for_scoring()

    print("\n\n\n==================== STEP 9: Recommendation Scoring for Saved ====================")
    rescore()

    print("\n\n\n==================== STEP 10: Running the Notifier script ====================")
    notify()

    print("\n\n\n==================== STEP 11: Postprocessing Tenders for Dates ====================")
    postprocessing()

    print("\n\n\n🎯 All steps completed successfully!")
