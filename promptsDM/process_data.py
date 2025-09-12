import pandas as pd
import os
import glob
import re
import unicodedata

def normalize_column_name(s):
    """
    Normalizes a string to be a valid column name by removing accents,
    converting to uppercase, and replacing non-alphanumeric characters with underscores.
    """
    if not isinstance(s, str):
        return s
    # NFD form allows for separation of base characters and diacritics
    nfkd_form = unicodedata.normalize('NFD', s)
    # Keep only non-diacritic characters
    only_ascii = u"".join([c for c in nfkd_form if not unicodedata.combining(c)])
    # Replace non-alphanumeric with underscores and convert to uppercase
    return re.sub(r'[\W_]+', '_', only_ascii).upper().strip('_')

def process_dataset(file_path, output_dir):
    """
    Processes a single dataset file to generate a summary report.
    """
    print(f"Processing file: {file_path}")

    # --- Metadata Extraction ---
    year, quarter = None, None
    # Pattern 1: ensu_YYYY_Qt
    match = re.search(r'ensu_(\d{4})_(\d)t', file_path, re.IGNORECASE)
    if match:
        year = int(match.group(1))
        quarter = int(match.group(2))
    else:
        # Pattern 2: ensu_cb_MMYY
        match = re.search(r'ensu_cb_(\d{2})(\d{2})', file_path, re.IGNORECASE)
        if match:
            month, year_short = int(match.group(1)), int(match.group(2))
            year = 2000 + year_short
            quarter = (month - 1) // 3 + 1
        else:
            # Pattern 3: ensu_MM_YYYY (for older files)
            match = re.search(r'ensu_(\d{2})_(\d{4})', file_path, re.IGNORECASE)
            if match:
                month, year = int(match.group(1)), int(match.group(2))
                quarter = (month - 1) // 3 + 1

    if year is None:
        print(f"  - WARNING: Could not extract Year and Quarter from path: {file_path}. Skipping.")
        return

    # --- Data Reading and Validation with Dynamic Column Mapping ---
    try:
        header_df = pd.read_csv(file_path, nrows=0, encoding='latin1')

        # Define potential column names
        ent_options = ['NOM_ENT', 'ENTIDAD', 'NOM_ENTIDAD', 'ENT']
        mun_options = ['NOM_MUN', 'CD']  # Using CD as a fallback for municipality
        perc_options = ['BP1_1', 'P1']

        # Find which columns are actually in the file
        ent_col = next((col for col in ent_options if col in header_df.columns), None)
        mun_col = next((col for col in mun_options if col in header_df.columns), None)
        perc_col = next((col for col in perc_options if col in header_df.columns), None)

        required_cols = [ent_col, mun_col, perc_col]
        if not all(required_cols):
            print(f"  - WARNING: Missing one or more key columns in {file_path}. Skipping.")
            return

        # Define the columns to use and how to rename them
        cols_to_use = {ent_col: 'NOM_ENT', mun_col: 'NOM_MUN', perc_col: 'BP1_1'}
        
        # Read the actual data
        df = pd.read_csv(file_path, usecols=cols_to_use.keys(), encoding='latin1', low_memory=False)
        df.rename(columns=cols_to_use, inplace=True)

        # Validate BP1_1 values
        valid_bp1_1 = [1, 2, 9]
        df['BP1_1'] = pd.to_numeric(df['BP1_1'], errors='coerce')
        df.dropna(subset=['BP1_1'], inplace=True)
        df['BP1_1'] = df['BP1_1'].astype(int)
        df = df[df['BP1_1'].isin(valid_bp1_1)]

    except Exception as e:
        print(f"  - ERROR: Could not read or validate file {file_path}. Reason: {e}. Skipping.")
        return

    if df.empty:
        print(f"  - WARNING: No valid data found in {file_path} after validation. Skipping.")
        return

    # --- Data Aggregation and Normalization ---
    df['NOM_ENT'] = df['NOM_ENT'].apply(normalize_column_name)
    df['NOM_MUN'] = df['NOM_MUN'].apply(normalize_column_name)

    grouped = df.groupby(['NOM_ENT', 'NOM_MUN'])

    summary = grouped.agg(
        TOTAL_REGISTROS=('BP1_1', 'size'),
        TOTAL_SEGUROS=('BP1_1', lambda x: (x == 1).sum()),
        TOTAL_INSEGUROS=('BP1_1', lambda x: (x == 2).sum()),
        TOTAL_NO_RESPONDE=('BP1_1', lambda x: (x == 9).sum())
    ).reset_index()

    # --- Percentage Calculation ---
    summary['PCT_SEGUROS'] = (summary['TOTAL_SEGUROS'] / summary['TOTAL_REGISTROS']) * 100
    summary['PCT_INSEGUROS'] = (summary['TOTAL_INSEGUROS'] / summary['TOTAL_REGISTROS']) * 100
    summary['PCT_NO_RESPONDE'] = (summary['TOTAL_NO_RESPONDE'] / summary['TOTAL_REGISTROS']) * 100

    # --- Add Metadata ---
    summary['AÑO'] = year
    summary['TRIMESTRE'] = quarter

    # --- Generate Output ---
    base_filename = os.path.basename(file_path)
    output_filename = os.path.splitext(base_filename)[0] + '_processed.csv'
    output_path = os.path.join(output_dir, output_filename)
    
    summary.to_csv(output_path, index=False)
    print(f"  - SUCCESS: Created processed file -> {output_path}")


def main():
    """
    Main function to find, filter, and process all relevant datasets.
    """
    # The script is in 'promptsDM', so paths should be relative to the project root.
    project_root = os.path.abspath('.')
    data_dir = os.path.join(project_root, 'data')
    output_dir = os.path.join(project_root, 'promptsDM')
    
    print(f"Starting search in: {data_dir}")
    
    # Find all files containing 'cb'
    candidate_files = glob.glob(os.path.join(data_dir, '**', '*cb*.csv'), recursive=True)
    
    print(f"Found {len(candidate_files)} candidate files.")

    # Filter out junk files
    valid_files = [
        f for f in candidate_files 
        if '__MACOSX' not in f 
        and 'diccionario_de_datos' not in f
        and os.path.basename(os.path.dirname(f)) == 'conjunto_de_datos'
    ]

    print(f"Found {len(valid_files)} valid data files to process.")

    for file_path in valid_files:
        process_dataset(file_path, output_dir)

    print("\nProcessing complete.")

if __name__ == '__main__':
    main()