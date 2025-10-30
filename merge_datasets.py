import pandas as pd
import numpy as np
from datasets import load_dataset, DatasetDict, Dataset
import json
import os

def get_file_paths():
    """Get file paths from user input with validation"""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Excel file path
    excel_path = input(f"Enter path to Excel file (or press Enter for default: {os.path.join(script_dir, 'ArPanEmo_train_clean.xlsx')}): ").strip()
    if not excel_path:
        excel_path = os.path.join(script_dir, "ArPanEmo_train_clean.xlsx")
    
    # JSONL directory
    jsonl_dir = input(f"Enter path to JSONL directory (or press Enter for default: {os.path.join(script_dir, 'collected_data')}): ").strip()
    if not jsonl_dir:
        jsonl_dir = os.path.join(script_dir, "collected_data")
    
    # Output file
    output_path = input(f"Enter output file path (or press Enter for default: {os.path.join(script_dir, 'merged_dataset.xlsx')}): ").strip()
    if not output_path:
        output_path = os.path.join(script_dir, "merged_dataset.xlsx")
    
    return excel_path, jsonl_dir, output_path


def process_excel_data(file_path):
    """Process the Excel file and clearly print columns."""
    import pandas as pd, numpy as np
    df = pd.read_excel(file_path)
    print("Loaded Excel file:", file_path)
    print("Columns:", df.columns.tolist())   # explicit and readable

    # Example mapping; adjust to your taste
    label_mapping = {
        'anger': 2, 'disgust': 2, 'fear': 2, 'pessimism': 2, 'sadness': 2,
        'happiness': 1, 'optimism': 1,
        'anticipation': 0, 'confusion': 0, 'neutral': 0, 'surprise': 0
    }

    processed = []
    for _, row in df.iterrows():
        orig = row.get('label') if 'label' in row else None
        mapped = label_mapping.get(str(orig).strip().lower(), 0) if pd.notna(orig) else np.nan
        processed.append({
            'source': 'youtube',
            'source_id': np.nan,
            'text': row.get('post', '') if 'post' in row else '',
            'label': mapped
        })
    print(f"Excel -> processed {len(processed)} rows")
    return pd.DataFrame(processed)


def process_arsas_dataset():
    """Robust ArSAS processing with clear debug & handling for Dataset vs DatasetDict."""
    import numpy as np
    try:
        dataset = load_dataset("arbml/ArSAS")
    except Exception as e:
        print("Error when calling load_dataset('arbml/ArSAS'):", e)
        raise

    print("Loaded HF dataset type:", type(dataset))
    # If it's a DatasetDict, list splits; if Dataset, wrap it under 'train'
    if isinstance(dataset, DatasetDict):
        splits = list(dataset.keys())
    elif isinstance(dataset, Dataset):
        # single-dataset case: wrap it
        splits = ['train']
        dataset = DatasetDict({'train': dataset})
    else:
        # fallback: attempt to iterate
        try:
            splits = list(dataset.keys())
        except Exception:
            raise RuntimeError("Unexpected dataset object returned by load_dataset()")

    print("Available splits in ArSAS:", splits)

    # mapping (robust normalization)
    label_mapping = {
        'positive': 1, 'pos': 1, 'negative': 2, 'neg': 2,
        'neutral': 0, 'mixed': 0
    }

    processed = []
    for split in splits:
        print("Processing split:", split)
        ds = dataset[split]  # now guaranteed to exist
        # show example structure
        try:
            first = next(iter(ds))
            print("Example item fields:", list(first.keys()))
        except StopIteration:
            print("  Split is empty:", split)
            continue
        except Exception as e:
            print("  Could not inspect first item:", e)

        for i, item in enumerate(ds):
            # item might be a dict-like or dataset.object — use get safely
            if not isinstance(item, dict):
                # convert to dict if needed
                try:
                    item = dict(item)
                except Exception:
                    continue
            tweet_id = item.get('Tweet_ID') or item.get('tweet_id') or item.get('id') or np.nan
            tweet_text = item.get('Tweet_text') or item.get('tweet_text') or item.get('text') or ''
            raw_label = item.get('label') or item.get('sentiment') or item.get('Sentiment_label_confidence') or ''
            # normalize label string
            raw_label_str = str(raw_label).strip().lower()
            mapped = label_mapping.get(raw_label_str, 0)

            processed.append({
                'source': 'twitter',
                'source_id': tweet_id,
                'text': tweet_text,
                'label': mapped
            })
            if i < 2:
                print(f"  sample {i}: id={tweet_id}, text_preview='{tweet_text[:50]}', raw_label={raw_label} -> mapped={mapped}")

    print("ArSAS -> processed", len(processed), "items")
    return pd.DataFrame(processed)


def process_tsac_dataset():
    """Process tunis-ai/tsac dataset from HuggingFace with better error handling"""
    try:
        dataset = load_dataset("tunis-ai/tsac")
        
        # Map TSAC labels to new scheme
        label_mapping = {
            'Positive': 1,
            'Negative': 2,
            'Neutral': 0
        }
        
        processed_data = []
        
        # Check which splits are available
        available_splits = list(dataset.keys())
        print(f"Available splits in TSAC: {available_splits}")
        
        for split in available_splits:
            print(f"Processing {split} split...")
            try:
                split_data = dataset[split]
                
                for i, item in enumerate(split_data):
                    try:
                        # Handle different possible field names
                        text = item.get('sentence', item.get('text', item.get('content', '')))
                        label = item.get('label', item.get('sentiment', ''))
                        
                        processed_data.append({
                            'source': 'facebook',
                            'source_id': np.nan,
                            'text': text,
                            'label': label_mapping.get(label, 0)
                        })
                        
                        # Print first few items for debugging
                        if i < 3:
                            print(f"  Sample {i}: text='{text[:50]}...', label={label}->{label_mapping.get(label, 0)}")
                            
                    except Exception as e:
                        print(f"  Error processing item {i} in {split}: {e}")
                        continue
                        
            except Exception as e:
                print(f"  Error processing split {split}: {e}")
                continue
        
        print(f"Successfully processed {len(processed_data)} items from TSAC")
        return pd.DataFrame(processed_data)
    
    except Exception as e:
        print(f"Error loading TSAC dataset: {e}")
        return pd.DataFrame()  # Return empty DataFrame if there's an error

def process_jsonl_files(jsonl_directory):
    """Process YouTube and Reddit JSONL files"""
    processed_data = []
    
    # Check if directory exists
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
                        
                        # Extract common fields with fallbacks
                        source = data.get('source', 'unknown')
                        source_id = data.get('id', data.get('source_id', np.nan))
                        text = data.get('text_raw', data.get('text', data.get('content', '')))
                        
                        # Only add if text is not empty
                        if text and text.strip():
                            processed_data.append({
                                'source': source,
                                'source_id': source_id,
                                'text': text.strip(),
                                'label': np.nan  # No labels in JSONL files
                            })
                    except json.JSONDecodeError as e:
                        print(f"Error parsing JSON in file {filename} at line {line_num}: {e}")
                        continue
                    except Exception as e:
                        print(f"Unexpected error processing {filename} at line {line_num}: {e}")
                        continue
    
    if not jsonl_files_found:
        print(f"No JSONL files found in {jsonl_directory}")
    else:
        print(f"Successfully processed {len(processed_data)} items from JSONL files")
    
    return pd.DataFrame(processed_data)

def merge_all_datasets(excel_path, jsonl_directory, output_path):
    """Merge all datasets into final unified format"""
    
    print("Processing Excel data...")
    excel_df = process_excel_data(excel_path)
    print(f"Excel data: {len(excel_df)} rows")
    
    print("Processing ArSAS dataset...")
    arsas_df = process_arsas_dataset()
    print(f"ArSAS data: {len(arsas_df)} rows")
    
    print("Processing TSAC dataset...")
    tsac_df = process_tsac_dataset()
    print(f"TSAC data: {len(tsac_df)} rows")
    
    print("Processing JSONL files...")
    jsonl_df = process_jsonl_files(jsonl_directory)
    print(f"JSONL data: {len(jsonl_df)} rows")
    
    # Check if we have any data to merge
    all_dfs = [df for df in [excel_df, arsas_df, tsac_df, jsonl_df] if len(df) > 0]
    
    if not all_dfs:
        print("Error: No data was processed from any source!")
        return pd.DataFrame()
    
    # Merge all datasets
    print("Merging all datasets...")
    final_df = pd.concat(all_dfs, ignore_index=True)
    
    # Convert NaN labels to 0 (unknown/neutral)
    final_df['label'] = final_df['label'].fillna(0).astype(int)
    
    # Remove duplicates based on text
    initial_count = len(final_df)
    final_df = final_df.drop_duplicates(subset=['text'])
    removed_duplicates = initial_count - len(final_df)
    print(f"Removed {removed_duplicates} duplicate texts")
    
    # Clean text - remove empty strings and very short texts
    final_df = final_df[final_df['text'].str.strip().str.len() > 5]
    print(f"After cleaning short texts: {len(final_df)} rows")
    
    # Save final dataset to Excel
    try:
        final_df.to_excel(output_path, index=False, engine='openpyxl')
        print(f"Final dataset saved to {output_path}")
        print(f"Total rows: {len(final_df)}")
        
        # Print detailed label information
        label_names = {0: 'Unknown/Neutral', 1: 'Positive', 2: 'Negative'}
        print("\nLabel distribution:")
        for label_value, count in final_df['label'].value_counts().sort_index().items():
            print(f"Label {label_value} ({label_names.get(label_value, 'Unknown')}): {count} rows")
        
        # Print source distribution
        print("\nSource distribution:")
        print(final_df['source'].value_counts())
        
    except Exception as e:
        print(f"Error saving to Excel: {e}")
        # Try to save as CSV as fallback
        try:
            csv_path = output_path.replace('.xlsx', '.csv')
            final_df.to_csv(csv_path, index=False, encoding='utf-8')
            print(f"Saved as CSV instead: {csv_path}")
        except Exception as e2:
            print(f"Also failed to save as CSV: {e2}")
    
    return final_df
        
def main():
    """Main function with better error handling"""
    try:
        # Get file paths
        excel_path, jsonl_dir, output_path = get_file_paths()
        
        # Check if input files exist
        if not os.path.exists(excel_path):
            print(f"Error: Excel file not found at {excel_path}")
            return
        
        print(f"Excel file: {excel_path}")
        print(f"JSONL directory: {jsonl_dir}")
        print(f"Output file: {output_path}")
        print("-" * 50)
        
        # Run the merge process
        final_dataset = merge_all_datasets(excel_path, jsonl_dir, output_path)
        
        if len(final_dataset) > 0:
            print("\nMerge completed successfully!")
        else:
            print("\nMerge failed - no data was processed")
        
    except Exception as e:
        print(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()
        print("Please check your file paths and try again.")

if __name__ == "__main__":
    main()