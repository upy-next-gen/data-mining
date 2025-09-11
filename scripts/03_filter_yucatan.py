import pandas as pd
import os
import glob
import unicodedata

def filter_yucatan():
    processed_files_path = 'data/yucatan_processed/*.csv'
    report_path = 'reports/dataset_yucatan.csv'

    # Find all processed CSV files
    all_files = glob.glob(processed_files_path)

    if not all_files:
        print(f"No processed files found in {os.path.dirname(processed_files_path)}")
        return

    # List to hold dataframes
    df_list = []

    for file in all_files:
        try:
            df = pd.read_csv(file, encoding='utf-8')
            # Clean up the column name if it has extra characters from previous steps
            df.columns = df.columns.str.strip()
            # Filter for Yucatan, being robust to whitespace and case
            # Also handle potential trailing characters like  seen in earlier outputs
            yucatan_df = df[df['NOM_ENT'].str.strip().str.contains('Yucat.n', case=False, regex=True)]
            if not yucatan_df.empty:
                df_list.append(yucatan_df)
        except Exception as e:
            print(f"Could not process {file}. Error: {e}")

    if not df_list:
        print("No records for 'Yucatan' found in any of the processed files.")
        return

    # Concatenate all dataframes
    final_df = pd.concat(df_list, ignore_index=True)

    print("Unique NOM_MUN before cleaning:", final_df['NOM_MUN'].unique())

    # Clean and standardize 'municipio' column
    final_df['NOM_MUN'] = final_df['NOM_MUN'].str.strip()
    final_df['NOM_MUN'] = final_df['NOM_MUN'].str.title() # Convert to Title Case
    final_df['NOM_MUN'] = final_df['NOM_MUN'].apply(lambda x: unicodedata.normalize('NFKD', x).encode('ascii', 'ignore').decode('utf-8')) # Normalize accents

    print("Unique NOM_MUN after cleaning:", final_df['NOM_MUN'].unique())

    # Save the final dataset
    final_df.to_csv(report_path, index=False, encoding='utf-8')
    print(f"Successfully created Yucatan dataset at: {report_path}")

if __name__ == '__main__':
    filter_yucatan()
