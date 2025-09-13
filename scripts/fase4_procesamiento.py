import os
import json
import logging
import pandas as pd
from datetime import datetime
# Assuming the helper script from phase 2 is available
import fase2_helpers as helpers

# --- Configuration ---
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
INPUT_JSON = os.path.join(BASE_DIR, 'temp', 'archivos_pendientes.json')
OUTPUT_DIR = os.path.join(BASE_DIR, 'data', 'yucatan_processed')
OUTPUT_JSON_SUMMARY = os.path.join(BASE_DIR, 'temp', 'procesamiento_resultados.json')
LOG_FILE = os.path.join(BASE_DIR, 'logs', f"fase4_procesamiento_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

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
    logging.info("--- Starting Phase 4: Processing Script ---")

    # Create output directory if it doesn't exist
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # 1. Read the Phase 3 pending files report
    try:
        with open(INPUT_JSON, 'r', encoding='utf-8') as f:
            pending_data = json.load(f)
    except FileNotFoundError:
        logging.error(f"Pending files report not found: {INPUT_JSON}. Please run Phase 3 first.")
        return

    files_to_process = pending_data.get('archivos_pendientes', [])
    logging.info(f"Found {len(files_to_process)} pending files to process.")

    processing_results = []

    for file_info in files_to_process:
        source_filepath = os.path.join(BASE_DIR, file_info['filepath'])
        periodo = file_info['periodo_str']
        output_filename = f"yucatan_security_{periodo}.csv"
        output_filepath = os.path.join(OUTPUT_DIR, output_filename)
        
        logging.info(f"Processing {source_filepath} for period {periodo}")

        try:
            # Read source CSV
            df = pd.read_csv(source_filepath, encoding='latin1', low_memory=False)

            # --- Data Cleaning and Filtering ---
            if 'NOM_CD' not in df.columns:
                df['NOM_CD'] = 'Sin ciudad'
            
            df['NOM_ENT'] = df['NOM_ENT'].apply(helpers.normalize_string)
            df['NOM_MUN'] = df['NOM_MUN'].apply(helpers.normalize_string)
            df['NOM_CD'] = df['NOM_CD'].apply(helpers.normalize_string)

            # Specific corrections for municipality names
            municipality_mapping = {
                'MARIDA': 'MERIDA',
                'KANASAN': 'KANASIN'
            }
            df['NOM_MUN'] = df['NOM_MUN'].replace(municipality_mapping)
            logging.info("Applied specific corrections for municipality names.")

            df_yucatan = df[df['NOM_ENT'] == 'YUCATAN'].copy()

            if df_yucatan.empty:
                logging.warning(f"No data for YUCATAN found in {source_filepath}, skipping.")
                processing_results.append({
                    "source_file": file_info['filepath'],
                    "output_file": output_filename,
                    "status": "Skipped",
                    "reason": "No Yucatan data after filtering"
                })
                continue

            # --- Aggregation and Calculation ---
            # Group by the desired columns and count BP1_1 values
            agg_data = df_yucatan.groupby(['NOM_ENT', 'NOM_MUN', 'NOM_CD'])['BP1_1'].value_counts().unstack(fill_value=0)
            
            # Rename columns to be more descriptive
            agg_data.rename(columns={
                1: 'TOTAL_SEGUROS',
                2: 'TOTAL_INSEGUROS',
                9: 'TOTAL_NO_RESPONDE'
            }, inplace=True)

            # Ensure all required columns exist, even if no data for them
            for col in ['TOTAL_SEGUROS', 'TOTAL_INSEGUROS', 'TOTAL_NO_RESPONDE']:
                if col not in agg_data.columns:
                    agg_data[col] = 0

            # Calculate totals and percentages
            agg_data['TOTAL_RESPUESTAS'] = agg_data['TOTAL_SEGUROS'] + agg_data['TOTAL_INSEGUROS'] + agg_data['TOTAL_NO_RESPONDE']
            
            agg_data['PORCENTAJE_SEGUROS'] = (agg_data['TOTAL_SEGUROS'] / agg_data['TOTAL_RESPUESTAS'] * 100).round(2)
            agg_data['PORCENTAJE_INSEGUROS'] = (agg_data['TOTAL_INSEGUROS'] / agg_data['TOTAL_RESPUESTAS'] * 100).round(2)
            agg_data['PORCENTAJE_NO_RESPONDE'] = (agg_data['TOTAL_NO_RESPONDE'] / agg_data['TOTAL_RESPUESTAS'] * 100).round(2)
            
            # Add metadata columns
            agg_data['PERIODO'] = periodo
            agg_data['FECHA_PROCESAMIENTO'] = datetime.now().isoformat(timespec='seconds')

            # Reset index to turn grouped columns back into regular columns
            final_df = agg_data.reset_index()

            # --- Save Processed CSV ---
            final_df.to_csv(output_filepath, index=False, encoding='utf-8')
            logging.info(f"Successfully created processed file: {output_filepath}")
            
            processing_results.append({
                "source_file": file_info['filepath'],
                "output_file": output_filename,
                "status": "Success",
                "rows_processed": len(final_df)
            })

        except Exception as e:
            logging.error(f"Failed to process {source_filepath}. Error: {e}", exc_info=True)
            processing_results.append({
                "source_file": file_info['filepath'],
                "output_file": output_filename,
                "status": "Error",
                "reason": str(e)
            })

    # --- Generate Final Summary Report ---
    final_report = {
        "timestamp": datetime.now().isoformat(timespec='seconds'),
        "total_files_to_process": len(files_to_process),
        "total_successful": sum(1 for r in processing_results if r['status'] == 'Success'),
        "total_errors_or_skipped": sum(1 for r in processing_results if r['status'] != 'Success'),
        "results": processing_results
    }

    try:
        with open(OUTPUT_JSON_SUMMARY, 'w', encoding='utf-8') as f:
            json.dump(final_report, f, indent=2, ensure_ascii=False)
        logging.info(f"Successfully created processing summary report: {OUTPUT_JSON_SUMMARY}")
    except Exception as e:
        logging.error(f"Failed to write processing summary report. Error: {e}")

    logging.info("--- Processing script finished ---")

if __name__ == "__main__":
    main()
