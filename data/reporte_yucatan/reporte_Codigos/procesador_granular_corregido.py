
import pandas as pd
import glob
import os
import re
import logging
import unicodedata

def setup_logging():
    """Configura un sistema de logging dual (archivo y consola)."""
    if logging.getLogger().hasHandlers():
        logging.getLogger().handlers.clear()

    log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    
    file_handler = logging.FileHandler('procesamiento_granular_corregido.log', mode='w', encoding='utf-8')
    file_handler.setFormatter(log_formatter)
    
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)
    
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

def normalize_string(s):
    """Normaliza un string: mayúsculas, sin acentos, Ñ a N, sin espacios extra."""
    if not isinstance(s, str):
        return s
    s = s.upper().strip()
    s = s.replace('Ñ', 'N')
    s = ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn')
    return s

def extract_metadata(filepath):
    """Extrae año y trimestre de la ruta del archivo."""
    basename = os.path.basename(filepath)
    match = re.search(r'_(\d{4})_(\d)t_', basename, re.IGNORECASE)
    if match: return int(match.group(1)), int(match.group(2))
    match = re.search(r'_(\d{2})_(\d{4})', basename, re.IGNORECASE)
    if match: return int(match.group(2)), (int(match.group(1)) - 1) // 3 + 1
    match = re.search(r'_cb_(\d{2})(\d{2})', basename, re.IGNORECASE)
    if match: return 2000 + int(match.group(2)), (int(match.group(1)) - 1) // 3 + 1
    logging.warning(f"No se pudieron extraer metadatos del nombre de archivo: {basename}")
    return None, None

def process_single_file(filepath):
    """Realiza el proceso ETL completo para un único archivo."""
    basename = os.path.basename(filepath)
    year, quarter = extract_metadata(filepath)
    if not year or not quarter:
        return

    logging.info(f"--- Iniciando procesamiento de: {basename} para el periodo {year}-Q{quarter} ---")
    
    try:
        df = pd.read_csv(filepath, encoding='utf-8', low_memory=False)
        logging.info(f"Archivo leído exitosamente con codificación UTF-8.")
    except UnicodeDecodeError:
        try:
            df = pd.read_csv(filepath, encoding='latin1', low_memory=False)
            logging.info(f"Archivo leído exitosamente con codificación latin1.")
        except Exception as e:
            logging.error(f"No se pudo leer el archivo. Error: {e}")
            return

    logging.info(f"Asumiendo para este archivo: BP1_1=1 (Seguro), BP1_1=2 (Inseguro), BP1_1=9 (No Responde).")
    
    required_cols = ['NOM_ENT', 'NOM_MUN', 'BP1_1']
    if not all(col in df.columns for col in required_cols):
        missing = [col for col in required_cols if col not in df.columns]
        logging.error(f"El archivo no contiene las columnas requeridas: {missing}. Saltando archivo.")
        return

    initial_rows = len(df)
    df.dropna(subset=['NOM_ENT', 'NOM_MUN'], inplace=True)
    if initial_rows > len(df):
        logging.warning(f"Se excluyeron {initial_rows - len(df)} filas con valores nulos en NOM_ENT o NOM_MUN.")

    df['NOM_ENT'] = df['NOM_ENT'].apply(normalize_string)
    df['NOM_MUN'] = df['NOM_MUN'].apply(normalize_string)

    df['BP1_1'] = pd.to_numeric(df['BP1_1'], errors='coerce')
    rows_before_na = len(df)
    df.dropna(subset=['BP1_1'], inplace=True)
    if rows_before_na > len(df):
        logging.warning(f"Se excluyeron {rows_before_na - len(df)} filas con valores no numéricos en BP1_1.")

    summary = df.groupby(['NOM_ENT', 'NOM_MUN']).apply(lambda g: pd.Series({
        'TOTAL_REGISTROS': len(g),
        'TOTAL_SEGUROS': (g['BP1_1'] == 1).sum(),
        'TOTAL_INSEGUROS': (g['BP1_1'] == 2).sum(),
        'TOTAL_NO_RESPONDE': (g['BP1_1'] == 9).sum()
    })).reset_index()

    summary['PCT_SEGUROS'] = ((summary['TOTAL_SEGUROS'] / summary['TOTAL_REGISTROS']) * 100).round(2)
    summary['PCT_INSEGUROS'] = ((summary['TOTAL_INSEGUROS'] / summary['TOTAL_REGISTROS']) * 100).round(2)
    summary['PCT_NO_RESPONDE'] = ((summary['TOTAL_NO_RESPONDE'] / summary['TOTAL_REGISTROS']) * 100).round(2)

    summary['AÑO'] = year
    summary['TRIMESTRE'] = quarter

    output_filename = f"procesado_{year}_Q{quarter}_cb.csv"
    output_path = os.path.join('data', 'yucatan-inseguridad', output_filename)
    
    final_df = summary[[
        'AÑO', 'TRIMESTRE', 'NOM_ENT', 'NOM_MUN', 'TOTAL_REGISTROS', 
        'TOTAL_SEGUROS', 'TOTAL_INSEGUROS', 'TOTAL_NO_RESPONDE',
        'PCT_SEGUROS', 'PCT_INSEGUROS', 'PCT_NO_RESPONDE'
    ]]
    
    final_df.to_csv(output_path, index=False, encoding='utf-8')
    logging.info(f"Archivo de resumen generado: {output_path} con {len(final_df)} registros.")

def main():
    """Función principal que orquesta todo el proceso."""
    setup_logging()
    
    output_dir = os.path.join('data', 'yucatan-inseguridad')
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        logging.info(f"Directorio de salida creado en: {output_dir}")

    search_pattern = os.path.join('data', '**', '*cb*.csv')
    all_files = glob.glob(search_pattern, recursive=True)
    
    valid_files = [f for f in all_files if '__MACOSX' not in f and 'diccionario_de_datos' not in f and 'conjunto_de_datos' in f]
    
    logging.info(f"Descubrimiento de archivos completado. Se encontraron {len(valid_files)} datasets para procesar.")
    
    for filepath in valid_files:
        try:
            process_single_file(filepath)
        except Exception as e:
            logging.critical(f"Error fatal al procesar {os.path.basename(filepath)}. Error: {e}", exc_info=True)
            
    logging.info(f"--- Proceso finalizado. ---")

if __name__ == '__main__':
    main()
