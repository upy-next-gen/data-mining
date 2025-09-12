import pandas as pd
import glob
import os

def filter_files_for_yucatan():
    """
    Overwrites processed CSV files to only contain rows where NOM_ENT is YUCATAN.
    """
    # The script is expected to be in the same directory as the processed files.
    path = os.path.dirname(os.path.abspath(__file__))
    processed_files = glob.glob(os.path.join(path, '*_processed.csv'))

    if not processed_files:
        print("No processed files found to filter.")
        return

    print(f"Found {len(processed_files)} files to filter for 'YUCATAN'.")

    for file_path in processed_files:
        try:
            df = pd.read_csv(file_path)
            
            # The NOM_ENT column should be normalized to uppercase with no accents.
            filtered_df = df[df['NOM_ENT'] == 'YUCATAN']
            
            if not filtered_df.empty:
                print(f"  - Filtering {os.path.basename(file_path)}... Found {len(filtered_df)} rows for YUCATAN.")
                filtered_df.to_csv(file_path, index=False)
            else:
                # If no YUCATAN rows, the file will become empty (just headers).
                print(f"  - Filtering {os.path.basename(file_path)}... No rows for YUCATAN found.")
                filtered_df.to_csv(file_path, index=False)

        except Exception as e:
            print(f"  - ERROR: Could not process file {file_path}. Reason: {e}")

    print("\nFiltering complete.")

if __name__ == '__main__':
    filter_files_for_yucatan()
