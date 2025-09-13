import pandas as pd
import glob
import os
import logging
import re
import unicodedata

# --- Configuration ---
DATA_DIR = '/home/danielgomez/Documents/data_mining/data-mining/data'
OUTPUT_DIR = '/home/danielgomez/Documents/data_mining/data-mining/data/yucatan-inseguridad'
CONSOLIDATED_DIR = '/home/danielgomez/Documents/data_mining/data-mining/reporte'
LOG_FILE = '/home/danielgomez/Documents/data_mining/data-mining/processing.log'
REQUIRED_COLUMNS = ['NOM_ENT', 'NOM_MUN', 'BP1_1']

# --- Setup Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, mode='w', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

def normalize_text(text):
    """Normalizes text by converting to uppercase, replacing Ñ, and removing accents."""
    if not isinstance(text, str):
        return text
    text = text.upper().replace('Ñ', 'N')
    return ''.join(
        c for c in unicodedata.normalize('NFD', text) 
        if unicodedata.category(c) != 'Mn'
    )

def process_file(file_path):
    """Processes a single dataset file, aggregates it, and saves the summary."""
    logging.info(f"--- Procesando: {file_path} ---")

    # --- Extract Year and Quarter ---
    year, quarter = 'YYYY', 'Q'

    # Try to find a 4-digit year (2015-2025)
    year_match = re.search(r'(201[5-9]|202[0-9])', file_path)
    if year_match:
        year = year_match.group(1)

    # Try to find a quarter indicator like "1t", "2t", etc.
    quarter_match = re.search(r'(\d)t', file_path, re.IGNORECASE)
    if quarter_match:
        quarter = quarter_match.group(1)
    else:
        # If no "t", try to find a month number to infer the quarter
        # Look for patterns like /03/, _06_, cb_09
        month_match = re.search(r'[_/](\d{2})[_/]|cb_(\d{2})', file_path)
        if month_match:
            # Extract the first non-None group
            month_str = next((g for g in month_match.groups() if g is not None), None)
            if month_str:
                month = int(month_str)
                if 1 <= month <= 3: quarter = '1'
                elif 4 <= month <= 6: quarter = '2'
                elif 7 <= month <= 9: quarter = '3'
                elif 10 <= month <= 12: quarter = '4'

    if year == 'YYYY' or quarter == 'Q':
        logging.warning(f"No se pudo extraer Año ({year}) y/o Trimestre ({quarter}) de: {file_path}.")

    # --- Read and Validate CSV ---
    try:
        try:
            df = pd.read_csv(file_path, encoding='utf-8', low_memory=False)
        except UnicodeDecodeError:
            logging.warning(f"Lectura con UTF-8 falló para {file_path}. Intentando con 'latin-1'.")
            df = pd.read_csv(file_path, encoding='latin-1', low_memory=False)
    except Exception as e:
        logging.error(f"No se pudo leer el archivo {file_path} con ninguna codificación. Error: {e}")
        return None

    if not all(col in df.columns for col in REQUIRED_COLUMNS):
        logging.error(f"El archivo {file_path} no contiene las columnas requeridas. Omitiendo.")
        return None

    # --- Normalize and Filter ---
    df['NOM_ENT'] = df['NOM_ENT'].apply(normalize_text)
    df['NOM_MUN'] = df['NOM_MUN'].apply(normalize_text)
    
    df_yucatan = df[df['NOM_ENT'] == 'YUCATAN'].copy()
    if df_yucatan.empty:
        logging.warning(f"No se encontraron datos para 'YUCATAN' en {file_path}. Omitiendo.")
        return None

    # --- Validate and Filter BP1_1 ---
    valid_bp1_1_values = [1, 2, 9]
    df_yucatan = df_yucatan[df_yucatan['BP1_1'].isin(valid_bp1_1_values)].copy()

    if df_yucatan.empty:
        logging.warning(f"No hay valores válidos en BP1_1 para YUCATAN en {file_path}. Omitiendo.")
        return None

    # --- Aggregate Data ---
    summary = df_yucatan.groupby('NOM_MUN').agg(
        TOTAL_REGISTROS=('BP1_1', 'size'),
        TOTAL_SEGUROS=('BP1_1', lambda x: (x == 1).sum()),
        TOTAL_INSEGUROS=('BP1_1', lambda x: (x == 2).sum()),
        TOTAL_NO_RESPONDE=('BP1_1', lambda x: (x == 9).sum())
    ).reset_index()

    # --- Calculate Percentages ---
    summary['PCT_SEGUROS'] = (summary['TOTAL_SEGUROS'] / summary['TOTAL_REGISTROS']) * 100
    summary['PCT_INSEGUROS'] = (summary['TOTAL_INSEGUROS'] / summary['TOTAL_REGISTROS']) * 100
    summary['PCT_NO_RESPONDE'] = (summary['TOTAL_NO_RESPONDE'] / summary['TOTAL_REGISTROS']) * 100

    # --- Add Year and Quarter ---
    summary['AÑO'] = year
    summary['TRIMESTRE'] = quarter
    
    # --- Save Individual Summary ---
    output_filename = f"procesado_{year}_{quarter}_cb.csv"
    output_path = os.path.join(OUTPUT_DIR, output_filename)
    try:
        summary.to_csv(output_path, index=False, encoding='utf-8')
        logging.info(f"Resumen guardado exitosamente en: {output_path}")
    except Exception as e:
        logging.error(f"No se pudo guardar el archivo de resumen {output_path}. Error: {e}")
    
    return summary

def main():
    """Main function to find and process all datasets."""
    logging.info("===== Iniciando Procesamiento de Datos de Yucatan ====")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(CONSOLIDATED_DIR, exist_ok=True)
    
    search_pattern = os.path.join(DATA_DIR, '**', '*cb*.csv')
    all_files = glob.glob(search_pattern, recursive=True)
    
    # Filter out dictionaries and previously processed files
    dataset_files = [
        f for f in all_files 
        if 'diccionario_de_datos' not in f.lower() and 'yucatan-inseguridad' not in f.lower()
    ]

    if not dataset_files:
        logging.warning("No se encontraron datasets para procesar.")
        return

    logging.info(f"Se encontraron {len(dataset_files)} datasets para procesar.")

    all_summaries = []
    for file_path in dataset_files:
        summary_df = process_file(file_path)
        if summary_df is not None:
            all_summaries.append(summary_df)

    if not all_summaries:
        logging.error("No se pudo generar ningún resumen. El proceso terminó sin datos.")
        return

    # --- Consolidate All Summaries ---
    logging.info("Consolidando todos los resúmenes...")
    consolidated_df = pd.concat(all_summaries, ignore_index=True)
    consolidated_output_path = os.path.join(CONSOLIDATED_DIR, 'consolidado_yucatan.csv')
    
    try:
        consolidated_df.to_csv(consolidated_output_path, index=False, encoding='utf-8')
        logging.info(f"Archivo consolidado guardado en: {consolidated_output_path}")
    except Exception as e:
        logging.error(f"No se pudo guardar el archivo consolidado. Error: {e}")

    logging.info("===== Procesamiento de Datos Finalizado =====")

if __name__ == '__main__':
    main()
