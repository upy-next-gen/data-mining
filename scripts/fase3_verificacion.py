import os
import json
import logging
from datetime import datetime

# --- Configuration ---
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
INPUT_JSON = os.path.join(BASE_DIR, 'temp', 'archivos_validados.json')
PROCESSED_DIR = os.path.join(BASE_DIR, 'data', 'yucatan_processed')
OUTPUT_JSON = os.path.join(BASE_DIR, 'temp', 'archivos_pendientes.json')
LOG_FILE = os.path.join(BASE_DIR, 'logs', f"fase3_verificacion_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

# --- Setup Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, mode='w'),
        logging.StreamHandler()
    ]
)

def get_expected_output_filename(file_info):
    """Constructs the expected output filename from the file's metadata."""
    periodo = file_info.get('periodo_str', 'Unknown')
    if periodo == 'Unknown':
        # Fallback for files where period couldn't be determined
        base_name = os.path.basename(file_info['filepath'])
        return f"procesado_{base_name}"
    return f"procesado_{periodo}_cb.csv"

def main():
    logging.info("--- Starting Phase 3: Incremental Verification Script ---")

    # Ensure the processed directory exists
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    logging.info(f"Verified existence of processed directory: {PROCESSED_DIR}")

    # 1. Read the Phase 2 validation report
    try:
        with open(INPUT_JSON, 'r', encoding='utf-8') as f:
            validation_data = json.load(f)
    except FileNotFoundError:
        logging.error(f"Validation file not found: {INPUT_JSON}. Please run Phase 2 first.")
        return

    # 2. Filter for processable files
    processable_files = [f for f in validation_data.get('validaciones', []) if f.get('es_procesable')]
    logging.info(f"Found {len(processable_files)} processable files in the validation report.")

    # 3. Get the list of already processed files
    try:
        already_processed_filenames = set(os.listdir(PROCESSED_DIR))
        logging.info(f"Found {len(already_processed_filenames)} files in the processed directory.")
    except Exception as e:
        logging.error(f"Could not read processed directory: {PROCESSED_DIR}. Error: {e}")
        return

    # 4. Compare and find pending files
    archivos_pendientes = []
    for file_info in processable_files:
        expected_filename = get_expected_output_filename(file_info)
        if expected_filename not in already_processed_filenames:
            archivos_pendientes.append(file_info)
            logging.info(f"File is pending: {file_info['filepath']} (Expected output: {expected_filename}) ")
    
    logging.info(f"Comparison complete. Found {len(archivos_pendientes)} pending files.")

    # 5. Generate a report of pending files
    final_report = {
        "timestamp": datetime.now().isoformat(timespec='seconds'),
        "total_archivos_pendientes": len(archivos_pendientes),
        "archivos_pendientes": archivos_pendientes
    }

    try:
        with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
            json.dump(final_report, f, indent=2, ensure_ascii=False)
        logging.info(f"Successfully created pending files report: {OUTPUT_JSON}")
    except Exception as e:
        logging.error(f"Failed to write pending files report. Error: {e}")

    logging.info("--- Verification script finished ---")

if __name__ == "__main__":
    main()
