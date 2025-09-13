
import pandas as pd
import glob
import os
import re
import logging
import unicodedata

def setup_logging():
    """Configura el logging para este script específico."""
    log_file = 'procesamiento_yucatan_filtrado.log'
    if logging.getLogger().hasHandlers():
        logging.getLogger().handlers.clear()

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, mode='w', encoding='utf-8'),
            logging.StreamHandler()
        ]
    )

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

def process_and_filter_file(filepath, output_dir):
    """Procesa un archivo y guarda un nuevo CSV solo si contiene datos de Yucatán."""
    basename = os.path.basename(filepath)
    year, quarter = extract_metadata(filepath)
    if not year or not quarter:
        return

    logging.info(f"--- Procesando: {basename} para el periodo {year}-Q{quarter} ---")
    
    try:
        df = pd.read_csv(filepath, encoding='utf-8', low_memory=False)
    except UnicodeDecodeError:
        df = pd.read_csv(filepath, encoding='latin1', low_memory=False)

    required_cols = ['NOM_ENT', 'NOM_MUN', 'BP1_1']
    if not all(col in df.columns for col in required_cols):
        logging.warning(f"El archivo no contiene las columnas requeridas. Saltando.")
        return

    # Normalización y filtrado PREVIO a la agregación
    df['NOM_ENT_NORMALIZED'] = df['NOM_ENT'].apply(normalize_string)
    df_yucatan = df[df['NOM_ENT_NORMALIZED'] == 'YUCATAN'].copy()

    if df_yucatan.empty:
        logging.info("No se encontraron datos de Yucatán en este archivo.")
        return

    logging.info(f"Se encontraron {len(df_yucatan)} registros de Yucatán. Procediendo a la agregación.")
    
    # Limpieza y validación sobre el subset de Yucatán
    initial_rows = len(df_yucatan)
    df_yucatan.dropna(subset=['NOM_MUN'], inplace=True)
    if initial_rows > len(df_yucatan):
        logging.warning(f"Se excluyeron {initial_rows - len(df_yucatan)} filas con NOM_MUN nulo.")

    df_yucatan['NOM_MUN'] = df_yucatan['NOM_MUN'].apply(normalize_string)
    df_yucatan['BP1_1'] = pd.to_numeric(df_yucatan['BP1_1'], errors='coerce')
    df_yucatan.dropna(subset=['BP1_1'], inplace=True)

    # Agregación
    summary = df_yucatan.groupby(['NOM_ENT', 'NOM_MUN']).apply(lambda g: pd.Series({
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

    # Guardado
    output_filename = f"procesado_{year}_Q{quarter}_yucatan.csv"
    output_path = os.path.join(output_dir, output_filename)
    
    final_df = summary[[
        'AÑO', 'TRIMESTRE', 'NOM_ENT', 'NOM_MUN', 'TOTAL_REGISTROS', 
        'TOTAL_SEGUROS', 'TOTAL_INSEGUROS', 'TOTAL_NO_RESPONDE',
        'PCT_SEGUROS', 'PCT_INSEGUROS', 'PCT_NO_RESPONDE'
    ]]
    
    final_df.to_csv(output_path, index=False, encoding='utf-8')
    logging.info(f"Archivo de Yucatán generado: {output_path} con {len(final_df)} registros.")

def main():
    """Función principal que orquesta todo el proceso."""
    setup_logging()
    
    output_dir = os.path.join('data', 'reporte_yucatan')
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        logging.info(f"Directorio de salida creado en: {output_dir}")

    search_pattern = os.path.join('data', '**', '*cb*.csv')
    all_files = glob.glob(search_pattern, recursive=True)
    valid_files = [f for f in all_files if '__MACOSX' not in f and 'diccionario_de_datos' not in f and 'conjunto_de_datos' in f]
    
    logging.info(f"Se analizarán {len(valid_files)} archivos de datos originales.")
    
    for filepath in valid_files:
        try:
            process_and_filter_file(filepath, output_dir)
        except Exception as e:
            logging.critical(f"Error fatal al procesar {os.path.basename(filepath)}. Error: {e}", exc_info=True)
            
    logging.info(f"--- Proceso finalizado. ---")

if __name__ == '__main__':
    main()
