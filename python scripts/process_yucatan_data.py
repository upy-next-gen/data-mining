
import os
import re
import glob
import pandas as pd
import logging
import unicodedata

# --- Configuration ---
LOG_FILE = 'processing.log'
OUTPUT_DIR = 'data/yucatan-inseguridad'
SOURCE_DIR = 'data'

# --- Column Mapping ---
# Maps year to a dictionary of {old_name: new_name}
COLUMN_MAPS = {
    2015: {'ENT': 'NOM_ENT', 'P1': 'BP1_1', 'CD': 'NOM_CD'}, # NOM_MUN seems absent
    2016: {'ENT': 'NOM_ENT', 'MUN': 'NOM_MUN', 'CD': 'NOM_CD', 'BP1_1': 'BP1_1'},
    2017: {'CD': 'NOM_CD'},
    2018: {'CD': 'NOM_CD'},
    2019: {'CD': 'NOM_CD'},
}

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, mode='w'),
        logging.StreamHandler()
    ]
)

# --- Helper Functions ---
def normalize_string(s):
    if not isinstance(s, str):
        return s
    s = s.upper()
    s = s.replace('Ñ', 'N')
    # Remove accents
    s = ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn')
    return s

def get_year_quarter(file_path):
    filename = os.path.basename(file_path)
    
    # This function attempts to extract year and quarter from a file path using regex.
    # It tries multiple patterns to match different naming conventions.

    # Pattern: YYYY_Qt (e.g., ...2018_1t...)
    match = re.search(r'(\d{4})_(\d)t', file_path, re.IGNORECASE)
    if match:
        return int(match.group(1)), int(match.group(2))

    # Pattern: MM_YYYY (e.g., ..._01_2015...)
    match = re.search(r'_(\d{2})_(\d{4})', filename, re.IGNORECASE)
    if match:
        month = int(match.group(1))
        year = int(match.group(2))
        if 1 <= month <= 3: return year, 1
        elif 4 <= month <= 6: return year, 2
        elif 7 <= month <= 9: return year, 3
        elif 10 <= month <= 12: return year, 4

    # Pattern: MMYY at the end of the filename (e.g., ...0322.csv for March 2022)
    match = re.search(r'(\d{2})(\d{2})\.csv', filename, re.IGNORECASE)
    if match:
        month, year_short = int(match.group(1)), int(match.group(2))
        year = 2000 + year_short
        if 1 <= month <= 3: return year, 1
        elif 4 <= month <= 6: return year, 2
        elif 7 <= month <= 9: return year, 3
        elif 10 <= month <= 12: return year, 4

    logging.warning(f'Could not determine year/quarter for {filename}')
    return None, None

# --- Main Processing Logic ---
def main():
    logging.info(f'Starting data processing. Output directory: {OUTPUT_DIR}')
    
    file_pattern = os.path.join(SOURCE_DIR, '**', 'conjunto_de_datos', '*cb*.csv')
    files = glob.glob(file_pattern, recursive=True)
    
    for file_path in files:
        if '__MACOSX' in file_path or os.path.basename(file_path).startswith('._') or OUTPUT_DIR in file_path:
            continue

        logging.info(f'--- Processing file: {file_path} ---')
        year, quarter = get_year_quarter(file_path)
        if not year or not quarter:
            continue

        try:
            df = pd.read_csv(file_path, encoding='latin1', low_memory=False)
        except Exception as e:
            logging.error(f'Could not read file {file_path}. Error: {e}')
            continue

        # --- Column Normalization ---
        original_cols = df.columns.tolist()
        if year in COLUMN_MAPS:
            logging.info(f'Applying column map for year {year}.')
            df.rename(columns=COLUMN_MAPS[year], inplace=True)
        
        logging.info(f'Original columns: {original_cols}')
        logging.info(f'New columns: {df.columns.tolist()}')

        # --- Column Validation ---
        if 'NOM_ENT' not in df.columns or 'BP1_1' not in df.columns:
            logging.error(f"Skipping file because it's missing essential columns 'NOM_ENT' or 'BP1_1'.")
            continue

        # --- Data Cleaning & Filtering ---
        if year < 2017:
            # Legacy files use numeric codes for ENT
            df['NOM_ENT'] = pd.to_numeric(df['NOM_ENT'], errors='coerce')
            df.dropna(subset=['NOM_ENT'], inplace=True)
            df['NOM_ENT'] = df['NOM_ENT'].astype(int)
            df = df[df['NOM_ENT'] == 31] # INEGI code for Yucatan
            df['NOM_ENT'] = 'YUCATAN' # Set the string name for aggregation
        else:
            # Modern files use string names
            df['NOM_ENT'] = df['NOM_ENT'].apply(normalize_string)
            df = df[df['NOM_ENT'] == 'YUCATAN']

        if df.empty:
            logging.info('No data for YUCATAN found in this file. Skipping.')
            continue
        
        initial_rows = len(df)
        logging.info(f'Found {initial_rows} rows for YUCATAN.')

        # Handle missing NOM_MUN
        if 'NOM_MUN' in df.columns:
            df.dropna(subset=['NOM_MUN'], inplace=True)
            if len(df) < initial_rows:
                logging.info(f'Dropped {initial_rows - len(df)} rows with missing NOM_MUN.')
                initial_rows = len(df)
            df['NOM_MUN'] = df['NOM_MUN'].apply(normalize_string)
            grouping_cols = ['NOM_ENT', 'NOM_MUN']
        else:
            logging.warning(f'NOM_MUN column not found for year {year}. Aggregating by NOM_ENT only.')
            grouping_cols = ['NOM_ENT']

        # Filter BP1_1
        df['BP1_1'] = pd.to_numeric(df['BP1_1'], errors='coerce')
        df.dropna(subset=['BP1_1'], inplace=True)
        df = df[df['BP1_1'].isin([1, 2, 9])]
        if len(df) < initial_rows:
            logging.info(f'Dropped {initial_rows - len(df)} rows with invalid BP1_1 values.')

        if df.empty:
            logging.info('No valid data remaining after cleaning. Skipping.')
            continue

        # --- Aggregation ---
        logging.info(f'Performing aggregation on {len(df)} rows.')
        result = df.groupby(grouping_cols).apply(lambda x: pd.Series({
            'TOTAL_REGISTROS': len(x),
            'TOTAL_SEGUROS': (x['BP1_1'] == 1).sum(),
            'TOTAL_INSEGUROS': (x['BP1_1'] == 2).sum(),
            'TOTAL_NO_RESPONDE': (x['BP1_1'] == 9).sum()
        })).reset_index()

        # --- Percentage Calculation ---
        result['PCT_SEGUROS'] = (result['TOTAL_SEGUROS'] / result['TOTAL_REGISTROS']) * 100
        result['PCT_INSEGUROS'] = (result['TOTAL_INSEGUROS'] / result['TOTAL_REGISTROS']) * 100
        result['PCT_NO_RESPONDE'] = (result['TOTAL_NO_RESPONDE'] / result['TOTAL_REGISTROS']) * 100

        result['AÑO'] = year
        result['TRIMESTRE'] = quarter

        # --- Save Output ---
        output_filename = f'procesado_{year}_t{quarter}_cb.csv'
        output_path = os.path.join(OUTPUT_DIR, output_filename)
        result.to_csv(output_path, index=False)
        logging.info(f'Successfully created output file: {output_path}')

    logging.info('--- Data processing finished. ---')

if __name__ == '__main__':
    main()
