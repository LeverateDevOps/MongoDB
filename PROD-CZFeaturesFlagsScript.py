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
SERVER_ENV_COLLECTION = "server_env"
SITE_COLLECTION = "site"

# Specify the desired output path for the JSON file
OUTPUT_JSON_FILE = r"C:\MongoDB\combined_analysis_results-PROD.json"

# ============================
# Main Function
# ============================

def main():
    try:
        # Step 1: Connect to MongoDB
        client = MongoClient(MONGO_CONNECTION_STRING, serverSelectionTimeoutMS=5000)  # 5 seconds timeout
        db = client[DATABASE_NAME]
        server_env_collection = db[SERVER_ENV_COLLECTION]
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
        # Step 2: Retrieve all ObjectIDs from server_env collection
        print("\nRetrieving all ObjectIDs from 'server_env' collection...")
        server_env_cursor = server_env_collection.find({}, {"_id": 1, "FEATURES_FLAGS": 1})
        
        # Create a mapping from _id to FEATURES_FLAGS for quick lookup
        server_env_id_to_flags = {}
        for doc in server_env_cursor:
            server_env_id = doc.get('_id')
            features_flags = doc.get('FEATURES_FLAGS', {})
            server_env_id_to_flags[server_env_id] = features_flags
        
        total_server_env_ids = len(server_env_id_to_flags)
        print(f"Total ObjectIDs retrieved from 'server_env': {total_server_env_ids}")

        if total_server_env_ids == 0:
            print("No ObjectIDs found in 'server_env' collection.")
            return

        # Step 3: Query 'site' collection for matching 'server_env_id' and 'disabled' = False
        print("\nQuerying 'site' collection for matching 'server_env_id' and 'disabled' = False...")
        matching_sites_cursor = site_collection.find(
            {
                "server_env_id": {"$in": list(server_env_id_to_flags.keys())},
                "disabled": False
            },
            {"server_env_id": 1, "FEATURES_FLAGS": 1}
        ).batch_size(100)  # Adjust batch size based on performance

        # Initialize counters and data structures
        disabled_false_count = 0
        feature_counts = defaultdict(lambda: {"true": 0, "false": 0})

        print("\nProcessing matching 'site' documents...")
        for site in matching_sites_cursor:
            disabled_false_count += 1
            server_env_id = site.get("server_env_id")
            
            # Retrieve FEATURES_FLAGS from server_env mapping
            features_flags = server_env_id_to_flags.get(server_env_id, {})
            
            # If FEATURES_FLAGS is not a dict, skip processing
            if not isinstance(features_flags, dict):
                print(f"Warning: FEATURES_FLAGS for server_env_id {server_env_id} is not a dict. Skipping.")
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
                    print(f"Warning: Feature '{feature}' for server_env_id {server_env_id} is not a boolean. Skipping.")

        # Step 4: Compile all results into a single dictionary
        results = {
            "total_identical_ids": total_server_env_ids,
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
