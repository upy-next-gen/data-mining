
import os
import json
import logging
import re
import pandas as pd
from datetime import datetime

# --- Configuration ---
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DATA_DIR = os.path.join(BASE_DIR, 'data', 'Archive')
OUTPUT_LOG_DIR = os.path.join(BASE_DIR, 'logs')
OUTPUT_TEMP_DIR = os.path.join(BASE_DIR, 'temp')
JSON_CATALOG_FILE = os.path.join(OUTPUT_TEMP_DIR, 'mapeo_archivos.json')
FILE_PATTERN = re.compile(r'(_cb_|conjunto_de_datos_cb|ensu_cb)', re.IGNORECASE)

def setup_logging():
    """Configures logging to file and stream."""
    os.makedirs(OUTPUT_LOG_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = os.path.join(OUTPUT_LOG_DIR, f"fase1_mapeo_{timestamp}.log")
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, mode='w'),
            logging.StreamHandler()
        ]
    )
    return log_filename

def find_files(root_dir):
    """Finds files matching the specified regex pattern."""
    matching_files = []
    logging.info(f"Starting file search in: {root_dir}")
    for root, _, files in os.walk(root_dir):
        for file in files:
            if file.endswith('.csv') and FILE_PATTERN.search(file):
                matching_files.append(os.path.join(root, file))
    logging.info(f"Found {len(matching_files)} matching CSV files.")
    return matching_files

def extract_period_from_path(file_path):
    """Extracts year and quarter from the file path using regex."""
    # Pattern for formats like 'ensu_2024_1t_csv'
    match = re.search(r'ensu_(\d{4})_(\dt)', file_path, re.IGNORECASE)
    if match:
        year, quarter_str = match.groups()
        return int(year), int(quarter_str[0])

    # Pattern for formats like 'ensu_cb_0322'
    match = re.search(r'ensu_cb_(\d{2})(\d{2})', file_path, re.IGNORECASE)
    if match:
        month, year_short = match.groups()
        year = int(f"20{year_short}")
        quarter = (int(month) - 1) // 3 + 1
        return year, quarter

    # Fallback for older formats like 'ensu_04_2017_csv'
    match = re.search(r'ensu_(\d{2})_(\d{4})', file_path, re.IGNORECASE)
    if match:
        quarter_num, year = match.groups()
        return int(year), int(quarter_num)

    logging.warning(f"Could not extract period from path: {file_path}")
    return None, None

def main():
    """Main script to catalog data files."""
    log_file = setup_logging()
    logging.info(f"Log file for this session: {log_file}")
    
    # Create temp directory
    os.makedirs(OUTPUT_TEMP_DIR, exist_ok=True)

    files_to_process = find_files(DATA_DIR)
    
    catalog_data = []

    for file_path in files_to_process:
        try:
            logging.info(f"Processing metadata for: {file_path}")
            
            # Basic file stats
            file_stat = os.stat(file_path)
            size_mb = round(file_stat.st_size / (1024 * 1024), 2)
            modified_date = datetime.fromtimestamp(file_stat.st_mtime).isoformat(timespec='seconds')
            
            # Extract period
            year, trimestre = extract_period_from_path(file_path)
            periodo_str = f"{year}_Q{trimestre}" if year and trimestre else "Unknown"

            # Get column info using pandas
            try:
                # Read only the header row to be efficient
                df_header = pd.read_csv(file_path, encoding='latin1', nrows=0, low_memory=False)
                columnas = list(df_header.columns)
                total_columnas = len(columnas)
                columnas_muestra = columnas[:5]
            except Exception as e:
                logging.error(f"Could not read columns from {file_path}. Error: {e}")
                columnas_muestra = []
                total_columnas = 0

            file_metadata = {
                "filepath": os.path.relpath(file_path, BASE_DIR),
                "filename": os.path.basename(file_path),
                "size_mb": size_mb,
                "modified_date": modified_date,
                "year": year,
                "trimestre": trimestre,
                "periodo_str": periodo_str,
                "identificacion_metodo": "contenido",
                "columnas_muestra": columnas_muestra,
                "total_columnas": total_columnas
            }
            catalog_data.append(file_metadata)

        except Exception as e:
            logging.error(f"Failed to process metadata for {file_path}. Error: {e}", exc_info=True)

    # Final JSON structure
    final_json = {
        "timestamp": datetime.now().isoformat(timespec='seconds'),
        "total_archivos": len(catalog_data),
        "archivos": catalog_data
    }

    # Write JSON output
    try:
        with open(JSON_CATALOG_FILE, 'w', encoding='utf-8') as f:
            json.dump(final_json, f, indent=2, ensure_ascii=False)
        logging.info(f"Successfully created JSON catalog at: {JSON_CATALOG_FILE}")
    except Exception as e:
        logging.error(f"Failed to write JSON catalog file. Error: {e}")

    logging.info("--- Mapping script finished ---")

if __name__ == "__main__":
    main()
