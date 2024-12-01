import json
import sys

if len(sys.argv) < 2:
    print("Please provide output folder as argument")
    sys.exit(1)

output_folder = sys.argv[1]

# Read the input JSON file
with open(f'{output_folder}/processed_articles_all.json', 'r') as f:
    data = json.load(f)

# Filter out items with "error" key
filtered_data = [item for item in data if "error" not in item]

# Write filtered items to new JSON file with timestamp
from datetime import datetime
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
with open(f'{output_folder}/filtered_articles_{timestamp}.json', 'w') as f:
    json.dump(filtered_data, f, indent=2)
