import os
import json
import logging
import pandas as pd
from datetime import datetime
import fase2_helpers as helpers

# --- Configuration ---
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
INPUT_JSON = os.path.join(BASE_DIR, 'temp', 'mapeo_archivos.json')
OUTPUT_JSON = os.path.join(BASE_DIR, 'temp', 'archivos_validados.json')
LOG_FILE = os.path.join(BASE_DIR, 'logs', f"fase2_validacion_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

CORE_COLS = ['NOM_ENT', 'NOM_MUN', 'BP1_1']

# --- Setup Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, mode='w'),
        logging.StreamHandler()
    ]
)

def main():
    logging.info("--- Starting Phase 2: Validation Script ---")
    
    try:
        with open(INPUT_JSON, 'r', encoding='utf-8') as f:
            catalog = json.load(f)
    except FileNotFoundError:
        logging.error(f"Input file not found: {INPUT_JSON}. Please run Phase 1 first.")
        return

    validations_results = []
    procesables_count = 0
    files_to_validate = [f for f in catalog['archivos'] if "diccionario" not in f['filename']]
    total_files_to_validate = len(files_to_validate)

    for file_info in files_to_validate:
        filepath = os.path.join(BASE_DIR, file_info['filepath'])
        logging.info(f"Validating file: {filepath}")

        try:
            df = pd.read_csv(filepath, encoding='latin1', low_memory=False)
            
            col_val = helpers.validate_columns(df.columns, CORE_COLS)
            
            if 'NOM_CD' not in df.columns:
                df['NOM_CD'] = 'Sin ciudad'

            bp1_1_val = helpers.validate_bp1_1(df)
            yucatan_val = helpers.validate_yucatan(df)

            es_procesable = (
                col_val['columnas_presentes'] and 
                bp1_1_val.get('valores_validos', False) and 
                yucatan_val.get('tiene_yucatan', False)
            )
            
            if es_procesable:
                procesables_count += 1

            result = {
                "filepath": file_info['filepath'],
                "periodo_str": file_info['periodo_str'],
                "validacion_columnas": col_val,
                "validacion_bp1_1": bp1_1_val,
                "validacion_yucatan": yucatan_val,
                "es_procesable": es_procesable
            }
            validations_results.append(result)

        except Exception as e:
            logging.error(f"Could not process file {filepath}. Error: {e}", exc_info=True)
            validations_results.append({
                "filepath": file_info['filepath'],
                "periodo_str": file_info['periodo_str'],
                "error": str(e),
                "es_procesable": False
            })

    final_report = {
        "timestamp": datetime.now().isoformat(timespec='seconds'),
        "total_archivos_analizados": total_files_to_validate,
        "procesables": procesables_count,
        "no_procesables": total_files_to_validate - procesables_count,
        "validaciones": validations_results
    }

    try:
        with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
            json.dump(final_report, f, indent=2, ensure_ascii=False)
        logging.info(f"Successfully created validation report: {OUTPUT_JSON}")
    except Exception as e:
        logging.error(f"Failed to write validation report. Error: {e}")

    logging.info("--- Validation script finished ---")

if __name__ == "__main__":
    main()
