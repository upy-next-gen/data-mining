import pandas as pd
import os
import glob
import logging
import re
import unicodedata

# --- Configuration ---
BASE_DIR = r"C:\Users\elian\downloads\Inseguridad_yucatan"
OUTPUT_DIR = os.path.join(BASE_DIR, "data", "yucatan-inseguridad")
LOG_FILE = os.path.join(BASE_DIR, "procesamiento.log")

# --- Setup Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, mode='w', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# --- Helper Functions ---
def normalize_text(text):
    if not isinstance(text, str):
        return text
    text = unicodedata.normalize('NFD', text)
    text = ''.join(c for c in text if unicodedata.category(c) != 'Mn').upper()
    text = text.replace('Ñ', 'N')
    return text

def extract_year_quarter(path):
    filename = os.path.basename(path)
    dir_path = os.path.dirname(path)

    # Pattern for format like _2023_1t_ or _2019_2t in filename
    match_t = re.search(r'_(\d{4})_(\d)t', filename)
    if match_t:
        year, quarter = match_t.groups()
        logging.info(f"Extracted Year={year}, Quarter={quarter} from {filename} (pattern: _YYYY_Qt_)")
        return year, quarter

    # Pattern for format like _03_2019 or _12_2021 in filename
    match_m = re.search(r'_(\d{2})_(\d{4})', filename)
    if match_m:
        month, year = match_m.groups()
        quarter_map = {
            '01': '1', '02': '1', '03': '1', '04': '2', '05': '2', '06': '2',
            '07': '3', '08': '3', '09': '3', '10': '4', '11': '4', '12': '4'
        }
        quarter = quarter_map.get(month, 'UNKNOWN')
        logging.info(f"Extracted Year={year}, Quarter={quarter} (from month {month}) from {filename} (pattern: _MM_YYYY)")
        return year, quarter

    # Pattern for format like _0322 or _1223 (MMYY) in filename
    match_mmyy = re.search(r'_(\d{2})(\d{2})', filename)
    if match_mmyy:
        month, year_short = match_mmyy.groups()
        year = f"20{year_short}"
        quarter_map = {
            '01': '1', '02': '1', '03': '1', '04': '2', '05': '2', '06': '2',
            '07': '3', '08': '3', '09': '3', '10': '4', '11': '4', '12': '4'
        }
        quarter = quarter_map.get(month, 'UNKNOWN')
        logging.info(f"Extracted Year={year}, Quarter={quarter} (from month {month}) from {filename} (pattern: _MMYY)")
        return year, quarter

    # New pattern for directory names like _ensu_01_2016_csv
    dir_name = os.path.basename(dir_path)
    match_dir = re.search(r'_ensu_(\d{2})_(\d{4})', dir_name)
    if match_dir:
        quarter, year = match_dir.groups()
        logging.info(f"Extracted Year={year}, Quarter={int(quarter)} from DIRECTORY {dir_name} (pattern: _ensu_QQ_YYYY)")
        return year, str(int(quarter)) # Return quarter as string

    logging.warning(f"Could not extract year and quarter from path {path}. Using generic names.")
    return f"UNKNOWN_{os.path.splitext(filename)[0]}", 'Q_UNKNOWN'

def process_modern_file(df, filepath):
    logging.info(f"Processing as MODERN file: {filepath}")
    df['NOM_ENT_NORMALIZED'] = df['NOM_ENT'].apply(normalize_text)
    df_yucatan = df[df['NOM_ENT_NORMALIZED'] == 'YUCATAN'].copy()
    if df_yucatan.empty:
        logging.warning(f"No data for 'YUCATAN' found in modern file {filepath}. Skipping.")
        return None
    df_yucatan['NOM_MUN_NORMALIZED'] = df_yucatan['NOM_MUN'].apply(normalize_text)
    return df_yucatan

def process_legacy_file(df, filepath):
    logging.info(f"Processing as LEGACY file: {filepath}")
    if 'CD' not in df.columns:
        logging.error(f"Legacy file {filepath} is missing 'CD' column. Skipping.")
        return None
    # In legacy files, CD is the key. 52 corresponds to Merida.
    df['CD'] = pd.to_numeric(df['CD'], errors='coerce')
    df_yucatan = df[df['CD'] == 52].copy()
    if df_yucatan.empty:
        logging.warning(f"No data for CD=52 (Merida) found in legacy file {filepath}. Skipping.")
        return None
    logging.info(f"Found {len(df_yucatan)} rows for CD=52 (Merida) in legacy file.")
    df_yucatan['NOM_ENT'] = 'YUCATAN'
    df_yucatan['NOM_MUN'] = 'MERIDA'
    df_yucatan['NOM_MUN_NORMALIZED'] = 'MERIDA'
    return df_yucatan

def process_file(filepath):
    logging.info(f"--- Starting processing for: {filepath} ---")
    year, quarter = extract_year_quarter(filepath)

    try:
        df = pd.read_csv(filepath, encoding='utf-8', low_memory=False)
    except UnicodeDecodeError:
        logging.warning(f"UTF-8 decoding failed for {filepath}. Trying with 'latin1' encoding.")
        try:
            df = pd.read_csv(filepath, encoding='latin1', low_memory=False)
        except Exception as e:
            logging.error(f"Could not read file {filepath}. Error: {e}")
            return
    except Exception as e:
        logging.error(f"Could not read file {filepath}. Error: {e}")
        return

    df_yucatan = None
    if all(col in df.columns for col in ['NOM_ENT', 'NOM_MUN', 'BP1_1']):
        df_yucatan = process_modern_file(df, filepath)
    elif all(col in df.columns for col in ['CD', 'BP1_1']):
        df_yucatan = process_legacy_file(df, filepath)
    else:
        logging.error(f"File {filepath} does not meet modern or legacy column requirements. Skipping.")
        return

    if df_yucatan is None or df_yucatan.empty:
        return

    valid_bp1_1_values = [1, 2, 9]
    initial_rows = len(df_yucatan)
    df_yucatan['BP1_1_NUMERIC'] = pd.to_numeric(df_yucatan['BP1_1'], errors='coerce')
    invalid_rows = df_yucatan[~df_yucatan['BP1_1_NUMERIC'].isin(valid_bp1_1_values)]
    
    if not invalid_rows.empty:
        logging.warning(f"Found {len(invalid_rows)} rows with invalid 'BP1_1' values. These will be excluded.")
    
    df_yucatan = df_yucatan[df_yucatan['BP1_1_NUMERIC'].isin(valid_bp1_1_values)].copy()
    df_yucatan['BP1_1'] = df_yucatan['BP1_1_NUMERIC'].astype(int)
    logging.info(f"Kept {len(df_yucatan)} rows out of {initial_rows} after cleaning 'BP1_1'.")

    if df_yucatan.empty:
        logging.warning(f"No valid data remained after cleaning BP1_1. Skipping.")
        return

    summary = df_yucatan.groupby('NOM_MUN_NORMALIZED').agg(
        TOTAL_REGISTROS=('BP1_1', 'count'),
        TOTAL_SEGUROS=('BP1_1', lambda x: (x == 1).sum()),
        TOTAL_INSEGUROS=('BP1_1', lambda x: (x == 2).sum()),
        TOTAL_NO_RESPONDE=('BP1_1', lambda x: (x == 9).sum())
    ).reset_index()

    summary['PCT_SEGUROS'] = (summary['TOTAL_SEGUROS'] / summary['TOTAL_REGISTROS'] * 100).round(2)
    summary['PCT_INSEGUROS'] = (summary['TOTAL_INSEGUROS'] / summary['TOTAL_REGISTROS'] * 100).round(2)
    summary['PCT_NO_RESPONDE'] = (summary['TOTAL_NO_RESPONDE'] / summary['TOTAL_REGISTROS'] * 100).round(2)

    summary['AÑO'] = year
    summary['TRIMESTRE'] = quarter
    
    original_names = df_yucatan[['NOM_ENT', 'NOM_MUN', 'NOM_MUN_NORMALIZED']].drop_duplicates('NOM_MUN_NORMALIZED')
    summary = pd.merge(summary, original_names, on='NOM_MUN_NORMALIZED', how='left')

    final_columns = [
        'NOM_ENT', 'NOM_MUN', 'TOTAL_REGISTROS', 'TOTAL_SEGUROS', 'TOTAL_INSEGUROS',
        'TOTAL_NO_RESPONDE', 'PCT_SEGUROS', 'PCT_INSEGUROS', 'PCT_NO_RESPONDE', 'AÑO', 'TRIMESTRE'
    ]
    for col in final_columns:
        if col not in summary.columns:
            summary[col] = pd.NA
    summary = summary[final_columns]

    output_filename = f"procesado_{year}_Q{quarter}_cb.csv"
    output_path = os.path.join(OUTPUT_DIR, output_filename)
    try:
        summary.to_csv(output_path, index=False, encoding='utf-8')
        logging.info(f"Successfully created processed file: {output_path}")
    except Exception as e:
        logging.error(f"Could not save processed file {output_path}. Error: {e}")

if __name__ == '__main__':
    logging.info("=== Script Start: Processing ENSU Datasets ===")
    search_pattern = os.path.join(BASE_DIR, "**", "*cb*.csv")
    files_to_process = glob.glob(search_pattern, recursive=True)

    if not files_to_process:
        logging.warning("No files matching the pattern '*cb*.csv' were found.")
    else:
        logging.info(f"Found {len(files_to_process)} files to process.")
        for filepath in files_to_process:
            if "procesado_" in os.path.basename(filepath):
                continue
            process_file(filepath)
    
    logging.info("=== Script End ===")