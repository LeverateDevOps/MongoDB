import os
import json
from pymongo import MongoClient
from collections import defaultdict
from pymongo.errors import ConnectionFailure, OperationFailure
from bson.objectid import ObjectId

# ============================
# Configuration Variables
# ============================

# MongoDB connection string
# IMPORTANT: Replace with your actual connection string or use environment variables for security
MONGO_CONNECTION_STRING = "mongodb+srv://kutas:0512d135b208c89ae1c871ffbaf7378f73f7706c7cf02b09e8bd11fd841dffa9@mongo-cz-prod.rqasa.mongodb.net/controlPanel?retryWrites=true&w=majority"

# Name of the database
DATABASE_NAME = "controlPanel"

# Names of the collections
platform_server_api_COLLECTION = "platform_server_api"
SITE_COLLECTION = "site"

# Specify the desired output path for the JSON file
OUTPUT_JSON_FILE = r"C:\MongoDB\Wt-FF-PROD.json"

# ============================
# Main Function
# ============================

def main():
    try:
        # Step 1: Connect to MongoDB
        client = MongoClient(MONGO_CONNECTION_STRING, serverSelectionTimeoutMS=5000)  # 5 seconds timeout
        db = client[DATABASE_NAME]
        platform_server_api_collection = db[platform_server_api_COLLECTION]
        site_collection = db[SITE_COLLECTION]
        
        # Test the connection
        client.admin.command('ping')
        print("Connected to MongoDB successfully.")
    except ConnectionFailure as cf:
        print(f"Could not connect to MongoDB: {cf}")
        return
    except OperationFailure as of:
        print(f"MongoDB operation failed: {of}")
        return
    except Exception as e:
        print(f"Unexpected error during MongoDB connection: {e}")
        return

    try:
        # Step 2: Retrieve all ObjectIDs from platform_server_api collection
        print("\nRetrieving all ObjectIDs from 'platform_server_api' collection...")
        platform_server_api_cursor = platform_server_api_collection.find({}, {"_id": 1, "WT_FEATURES_FLAGS": 1})
        
        # Create a mapping from _id to FEATURES_FLAGS for quick lookup
        platform_server_api_id_to_flags = {}
        for doc in platform_server_api_cursor:
            platform_server_api_id = doc.get('_id')
            features_flags = doc.get('WT_FEATURES_FLAGS', {})
            platform_server_api_id_to_flags[platform_server_api_id] = features_flags
        
        total_platform_server_api_ids = len(platform_server_api_id_to_flags)
        print(f"Total ObjectIDs retrieved from 'platform_server_api': {total_platform_server_api_ids}")

        if total_platform_server_api_ids == 0:
            print("No ObjectIDs found in 'platform_server_api' collection.")
            return

        # Step 3: Query 'site' collection for matching 'platform_server_api_id' and 'disabled' = False
        print("\nQuerying 'site' collection for matching 'platform_server_api_id' and 'disabled' = False...")
        matching_sites_cursor = site_collection.find(
            {
                "platform_server_api_id": {"$in": list(platform_server_api_id_to_flags.keys())},
                "disabled": False
            },
            {"platform_server_api_id": 1, "FEATURES_FLAGS": 1}
        ).batch_size(100)  # Adjust batch size based on performance

        # Initialize counters and data structures
        disabled_false_count = 0
        feature_counts = defaultdict(lambda: {"true": 0, "false": 0})

        print("\nProcessing matching 'site' documents...")
        for site in matching_sites_cursor:
            disabled_false_count += 1
            platform_server_api_id = site.get("platform_server_api_id")
            
            # Retrieve FEATURES_FLAGS from platform_server_api mapping
            features_flags = platform_server_api_id_to_flags.get(platform_server_api_id, {})
            
            # If FEATURES_FLAGS is not a dict, skip processing
            if not isinstance(features_flags, dict):
                print(f"Warning: FEATURES_FLAGS for platform_server_api_id {platform_server_api_id} is not a dict. Skipping.")
                continue

            # Iterate through each feature flag and count
            for feature, value in features_flags.items():
                if isinstance(value, bool):
                    if value:
                        feature_counts[feature]["true"] += 1
                    else:
                        feature_counts[feature]["false"] += 1
                else:
                    # Handle non-boolean values if necessary
                    print(f"Warning: Feature '{feature}' for platform_server_api_id {platform_server_api_id} is not a boolean. Skipping.")

        # Step 4: Compile all results into a single dictionary
        results = {
            "total_identical_ids": total_platform_server_api_ids,
            "disabled_false_sites_count": disabled_false_count,
            "feature_flags_counts": feature_counts
        }

        # Step 5: Save the results to a single JSON file
        save_json(results, OUTPUT_JSON_FILE)
        print(f"\nAll results have been saved to {OUTPUT_JSON_FILE}.")

    except OperationFailure as of:
        print(f"MongoDB operation failed during processing: {of}")
    except Exception as e:
        print(f"An error occurred during processing: {e}")
    finally:
        # Close the MongoDB connection
        client.close()
        print("\nMongoDB connection closed.")

# ============================
# Helper Function to Save JSON
# ============================

def save_json(data, filepath):
    """
    Saves the provided data to the specified JSON file.
    Ensures that the directory exists; creates it if necessary.
    
    Parameters:
        data (dict): The data to be saved.
        filepath (str): The path to the JSON file.
    """
    try:
        output_dir = os.path.dirname(filepath)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)
            print(f"Created directory: {output_dir}")

        # Convert defaultdict to regular dict for JSON serialization
        if isinstance(data.get("feature_flags_counts"), defaultdict):
            data["feature_flags_counts"] = {k: v for k, v in data["feature_flags_counts"].items()}

        with open(filepath, "w", encoding='utf-8') as json_file:
            json.dump(data, json_file, indent=4)
        print(f"All results have been saved to {filepath}.")
    except Exception as e:
        print(f"Failed to save JSON to {filepath}: {e}")

# ============================
# Entry Point
# ============================

if __name__ == "__main__":
    main()
