import pandas as pd
import glob
import os

def consolidate_yucatan_data():
    """
    Finds all processed files, filters for Yucatan data, and consolidates them
    into a single CSV file.
    """
    # The script is expected to be in the same directory as the processed files.
    path = os.path.dirname(os.path.abspath(__file__))
    processed_files = glob.glob(os.path.join(path, '*_processed.csv'))

    if not processed_files:
        print("No processed files found to consolidate.")
        return

    print(f"Found {len(processed_files)} processed files to consolidate.")

    all_dataframes = []
    for file_path in processed_files:
        try:
            df = pd.read_csv(file_path)
            # Ensure the dataframe is not empty and contains Yucatan data
            if not df.empty and 'YUCATAN' in df['NOM_ENT'].unique():
                all_dataframes.append(df)
        except Exception as e:
            print(f"  - WARNING: Could not process file {file_path}. Reason: {e}")

    if not all_dataframes:
        print("No data for YUCATAN found in any of the processed files.")
        return

    # Concatenate all dataframes into one
    consolidated_df = pd.concat(all_dataframes, ignore_index=True)

    # Sort the data for better presentation in the dashboard
    consolidated_df.sort_values(by=['AÑO', 'TRIMESTRE', 'NOM_MUN'], inplace=True)

    # Save the final consolidated file
    output_path = os.path.join(path, 'yucatan_consolidated_data.csv')
    consolidated_df.to_csv(output_path, index=False)
    print(f"\nSuccessfully created consolidated data file: {output_path}")

if __name__ == '__main__':
    consolidate_yucatan_data()
