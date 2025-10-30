import pandas as pd
import numpy as np
import json
import os

def get_jsonl_directory():
    """Get JSONL directory path from user input with validation"""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    jsonl_dir = input(f"Enter path to JSONL directory (or press Enter for default: {os.path.join(script_dir, 'collected_data')}): ").strip()
    if not jsonl_dir:
        jsonl_dir = os.path.join(script_dir, "collected_data")
    return jsonl_dir

def process_jsonl_files(jsonl_directory):
    """Process YouTube and Reddit JSONL files, label always set to 0"""
    processed_data = []

    if not os.path.exists(jsonl_directory):
        print(f"Warning: JSONL directory '{jsonl_directory}' does not exist")
        return pd.DataFrame(processed_data)

    jsonl_files_found = False
    for filename in os.listdir(jsonl_directory):
        if filename.endswith('.jsonl'):
            jsonl_files_found = True
            file_path = os.path.join(jsonl_directory, filename)
            print(f"Processing {filename}...")

            with open(file_path, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    try:
                        data = json.loads(line.strip())
                        source = data.get('source', 'unknown')
                        source_id = data.get('id', data.get('source_id', np.nan))
                        text = data.get('text_raw', data.get('text', data.get('content', '')))

                        if text and text.strip():
                            processed_data.append({
                                'source': source,
                                'source_id': source_id,
                                'text': text.strip(),
                                'label': 0  # Always 0
                            })
                    except json.JSONDecodeError as e:
                        print(f"Error parsing JSON in file {filename} at line {line_num}: {e}")
                    except Exception as e:
                        print(f"Unexpected error processing {filename} at line {line_num}: {e}")

    if not jsonl_files_found:
        print(f"No JSONL files found in {jsonl_directory}")
    else:
        print(f"Successfully processed {len(processed_data)} items from JSONL files")

    return pd.DataFrame(processed_data)

def main():
    jsonl_dir = get_jsonl_directory()
    output_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "collected_dataset.xlsx")
    print(f"JSONL directory: {jsonl_dir}")
    print(f"Output file will be: {output_path}")

    df = process_jsonl_files(jsonl_dir)

    if len(df) > 0:
        try:
            df.to_excel(output_path, index=False, engine='openpyxl')
            print(f"Dataset saved to {output_path} with {len(df)} rows (all labels = 0)")
        except Exception as e:
            print(f"Error saving Excel file: {e}")
    else:
        print("No data processed from JSONL files.")

if __name__ == "__main__":
    main()
