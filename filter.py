import json
from datetime import datetime

def filter_articles(input_folder = 'gm_single_20241130_230030', output_folder = 'gm_single_OXY'):
    # Read the input JSON file
    with open(f'{input_folder}/processed_articles_all.json', 'r') as f:
        data = json.load(f)

    # Filter out items with "error" key
    filtered_data = [item for item in data if "error" not in item]

    # Write filtered items to new JSON file with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    with open(f'{output_folder}/filtered_articles_{timestamp}.json', 'w') as f:
        json.dump(filtered_data, f, indent=2)

if __name__ == "__main__":
    filter_articles()