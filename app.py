import boto3
import json
import os

# Fetch items (if needed) and extract unique keys structure
# table_name = "NamedropAccounts"
table_name = "SPECIFY_TABLE_NAME"
profile_name = "SPECIFY_PROFILE_NAME"


def download_items_to_file(table_name, profile_name, filename=None):
    # If filename is not provided, save it in the same directory as the script
    if filename is None:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        filename = os.path.join(current_dir, "dynamodb_items" + table_name + ".json")

    # Initialize a boto3 session with the specified profile
    session = boto3.Session(profile_name=profile_name)

    # Create a DynamoDB client using the session
    dynamodb = session.client("dynamodb")

    # List to store all items
    all_items = []

    # Scan the DynamoDB table to retrieve all items
    paginator = dynamodb.get_paginator("scan")

    for page in paginator.paginate(TableName=table_name):
        all_items.extend(page["Items"])

    # Save items to a JSON file
    with open(filename, "w") as f:
        json.dump(all_items, f)

    print(f"Items saved to {filename}")


def dynamodb_to_json(item):
    """Converts a DynamoDB item to regular JSON format."""
    json_item = {}
    for key, value in item.items():
        for dtype, data_value in value.items():
            if dtype == "M":
                json_item[key] = dynamodb_to_json(data_value)
            elif dtype in ["S", "N", "BOOL"]:
                json_item[key] = data_value
    return json_item


def fetch_keys_structure(table_name, profile_name, filename=None):
    # Determine the filename
    if filename is None:
        current_dir = os.getcwd()
        filename = os.path.join(current_dir, "dynamodb_items" + table_name + ".json")

    # Check if file exists
    if not os.path.exists(filename):
        # Download items and save to file if file doesn't exist
        download_items_to_file(table_name, profile_name, filename)

    # Load items from the JSON file
    with open(filename, "r") as f:
        dynamodb_items = json.load(f)

    # Convert DynamoDB items to regular JSON format
    json_items = [dynamodb_to_json(item) for item in dynamodb_items]

    # Initialize a dictionary to store unique keys structure
    unique_structure = {}

    for item in json_items:
        item_structure = extract_structure(item)
        deep_merge_dicts(unique_structure, item_structure)

    return unique_structure


def fetch_keys_structure(table_name, profile_name, filename=None):
    # Determine the filename
    if filename is None:
        current_dir = os.getcwd()
        filename = os.path.join(current_dir, "dynamodb_items" + table_name + ".json")

    # Check if file exists
    if not os.path.exists(filename):
        # Download items and save to file if file doesn't exist
        download_items_to_file(table_name, profile_name, filename)

    # Load items from the JSON file
    with open(filename, "r") as f:
        dynamodb_items = json.load(f)

    # Convert DynamoDB items to regular JSON format
    json_items = [dynamodb_to_json(item) for item in dynamodb_items]

    # Initialize a dictionary to store unique keys structure
    unique_structure = {}

    for item in json_items:
        item_structure = extract_structure(item)
        deep_merge_dicts(unique_structure, item_structure)

    return unique_structure


def extract_structure(item):
    structure = {}
    for key, value in item.items():
        if isinstance(value, dict):
            nested_structure = extract_structure(value)
            structure[key] = nested_structure
        elif isinstance(value, str):
            structure[key] = "STRING"
        elif isinstance(value, bool):
            structure[key] = "BOOLEAN"
        elif isinstance(value, (int, float)):
            structure[key] = "NUMBER"
        else:
            structure[key] = "UNKNOWN"
    return structure


def deep_merge_dicts(dict1, dict2):
    """Recursively merges dict2 into dict1."""
    for key, value in dict2.items():
        if key in dict1 and isinstance(dict1[key], dict) and isinstance(value, dict):
            deep_merge_dicts(dict1[key], value)
        else:
            dict1[key] = value
    return dict1


def extract_structure_with_type_format1(item, required_keys):
    structure = {}
    for key, value in item.items():
        if isinstance(value, dict):
            nested_structure = extract_structure_with_type_format1(value, required_keys)
            if nested_structure:
                structure[key] = nested_structure
            else:
                structure[key] = "UNKNOWN"
        elif isinstance(value, str) and "url" in key.lower():
            structure[key] = "**URL**" if key in required_keys else "URL"
        elif isinstance(value, str):
            structure[key] = "**STRING**" if key in required_keys else "STRING"
        elif isinstance(value, bool):
            structure[key] = "**BOOLEAN**" if key in required_keys else "BOOLEAN"
        elif isinstance(value, (int, float)):
            structure[key] = "**NUMBER**" if key in required_keys else "NUMBER"
        else:
            structure[key] = "UNKNOWN"
    return structure


def determine_required_keys(items, timestamp_key="dateUpdated", percentage=90):
    """Determine which keys are required based on their appearance in the most recent items."""

    # Sort items by timestamp, newest first
    sorted_items = sorted(items, key=lambda x: x.get(timestamp_key, ""), reverse=True)

    # Determine the number of recent items to consider
    num_recent_items = len(sorted_items) * percentage // 100

    # Take the subset of the most recent items
    recent_items = sorted_items[:num_recent_items]

    key_counts = {}  # Dictionary to store the count of each key's appearance

    # Count the appearance of each key in the recent items
    for item in recent_items:
        for key in item.keys():
            key_counts[key] = key_counts.get(key, 0) + 1

    # Determine required keys (keys that appear in every recent item)
    required_keys = {
        key for key, count in key_counts.items() if count == num_recent_items
    }

    return required_keys


def extract_structure_with_comments_format2(item, required_keys):
    # TODO: use OpenAI to generate Comments on-demand rather than manually
    comments = {
        "key" : "comment to describe",
        "key2" : "comment to describe"
    }

    type_placeholders = {
        "STRING": "STRING",
        "URL": "URL",
        "BOOLEAN": "BOOLEAN",
        "NUMBER": "NUMBER",
        "UNKNOWN": "UNKNOWN",
    }

    structure = "{\n"
    for key, value in item.items():
        if isinstance(value, dict):
            nested_structure = extract_structure_with_comments_format2(
                value, required_keys
            )
            structure += f'    "{key}": {nested_structure},\n'
        else:
            placeholder = type_placeholders.get(value, "UNKNOWN")
            if key in required_keys:
                placeholder = f"**{placeholder}**"
            comment = comments.get(key, "")
            # Logic to detect URL type based on key name
            if isinstance(value, str) and "url" in key.lower():
                placeholder = "**URL**" if key in required_keys else "URL"
            structure += f'    "{key}": "{placeholder}", // {comment}\n'
    structure += "}"
    return structure


# Construct the filename dynamically based on the table name
filename = "dynamodb_items" + table_name + ".json"

keys_structure = fetch_keys_structure(table_name, profile_name)

# Convert DynamoDB items to regular JSON format
with open(filename, "r") as f:
    dynamodb_items = json.load(f)
json_items = [dynamodb_to_json(item) for item in dynamodb_items]

# Determine required keys from the items
required_keys = determine_required_keys(json_items)

# Extract structure using the new format considering required keys
keys_structure_format1 = extract_structure_with_type_format1(
    keys_structure, required_keys
)
print("Format 1:\n")
print(json.dumps(keys_structure_format1, indent=4))

# Extract the structure in the second format with comments
keys_structure_format2 = extract_structure_with_comments_format2(
    keys_structure, required_keys
)
print("\n\nFormat 2:\n")
print(keys_structure_format2, indent=4)
