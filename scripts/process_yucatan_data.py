import os
import glob
import logging
import pandas as pd
import unicodedata
import re

# --- Configuración Dinámica de Rutas ---
# Obtiene la ruta del directorio donde se encuentra el script
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))

# Construye las rutas de forma relativa al proyecto
INPUT_DATA_DIR = os.path.join(PROJECT_ROOT, "data")
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "data", "yucatan-inseguridad")
LOG_DIR = os.path.join(PROJECT_ROOT, "logs")
LOG_FILE = os.path.join(LOG_DIR, "yucatan_processing.log")

# --- Creación de Directorios ---
# Asegurarse de que los directorios de logs y salida existan
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# --- Configuración de Logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, mode='w', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# --- Constantes de Procesamiento ---
TARGET_ENTITY = "YUCATAN"
REQUIRED_COLS_RAW = ["NOM_ENT", "NOM_MUN", "BP1_1"]
VALID_BP1_1_VALUES = [1, 2, 9]


# --- Funciones de Normalización ---
def normalize_text(text):
    """Normaliza un string a mayúsculas, sin acentos y reemplazando Ñ por N."""
    if not isinstance(text, str):
        return text
    text = text.upper()
    text = text.replace('Ñ', 'N')
    text = ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')
    return text.strip()

def normalize_col_name(col_name):
    """Normaliza el nombre de una columna para facilitar la comparación."""
    return normalize_text(col_name).replace(' ', '_')

# --- Lógica Principal ---
def process_dataset(file_path):
    """Procesa un único archivo de dataset."""
    logging.info(f"--- Iniciando procesamiento para: {file_path} ---")
    
    try:
        # Patrón de regex más robusto para extraer año y trimestre
        match = re.search(r'_(\d{4})_(\d{1})t', file_path)
        if not match:
            logging.warning(f"No se pudo extraer año y trimestre de {os.path.basename(file_path)}. Saltando archivo.")
            return
        
        year, quarter = match.groups()
        logging.info(f"Año extraído: {year}, Trimestre: {quarter}")

    except Exception as e:
        logging.error(f"Error al extraer metadatos de {os.path.basename(file_path)}: {e}. Saltando archivo.")
        return

    try:
        df = pd.read_csv(file_path, encoding='utf-8', low_memory=False)
    except UnicodeDecodeError:
        logging.warning(f"Error de codificación UTF-8 en {os.path.basename(file_path)}. Intentando con 'latin1'.")
        try:
            df = pd.read_csv(file_path, encoding='latin1', low_memory=False)
        except Exception as e:
            logging.error(f"No se pudo leer el archivo {os.path.basename(file_path)} con ninguna codificación. Error: {e}")
            return
    except Exception as e:
        logging.error(f"Error al leer el archivo CSV {os.path.basename(file_path)}: {e}")
        return

    initial_rows = len(df)
    logging.info(f"Archivo leído. Número inicial de registros: {initial_rows}")

    df.columns = [normalize_col_name(col) for col in df.columns]
    
    required_cols_norm = [normalize_col_name(col) for col in REQUIRED_COLS_RAW]
    if not all(col in df.columns for col in required_cols_norm):
        missing_cols = [col for col in required_cols_norm if col not in df.columns]
        logging.error(f"Faltan columnas requeridas en {os.path.basename(file_path)}: {missing_cols}. Saltando archivo.")
        return

    # --- Limpieza y Filtrado ---
    df['NOM_ENT'] = df['NOM_ENT'].apply(normalize_text)
    df_filtered = df[df['NOM_ENT'] == TARGET_ENTITY].copy()
    rows_after_entity_filter = len(df_filtered)
    logging.info(f"Filtrado por entidad '{TARGET_ENTITY}'. Registros restantes: {rows_after_entity_filter}")

    if rows_after_entity_filter == 0:
        logging.warning(f"No se encontraron registros para '{TARGET_ENTITY}' en {os.path.basename(file_path)}. No se generará archivo.")
        return

    key_cols = ['NOM_ENT', 'NOM_MUN']
    rows_before_na = len(df_filtered)
    df_filtered.dropna(subset=key_cols, inplace=True)
    rows_after_na = len(df_filtered)
    if rows_before_na > rows_after_na:
        logging.info(f"Se eliminaron {rows_before_na - rows_after_na} filas por valores nulos en {key_cols}.")

    rows_before_bp1_1_filter = len(df_filtered)
    df_filtered = df_filtered[df_filtered['BP1_1'].isin(VALID_BP1_1_VALUES)]
    rows_after_bp1_1_filter = len(df_filtered)
    if rows_before_bp1_1_filter > rows_after_bp1_1_filter:
        logging.info(f"Se eliminaron {rows_before_bp1_1_filter - rows_after_bp1_1_filter} filas por valores inválidos en BP1_1.")

    if df_filtered.empty:
        logging.warning(f"El dataset quedó vacío después de la limpieza. No se generará archivo para {os.path.basename(file_path)}.")
        return
        
    df_filtered['NOM_MUN'] = df_filtered['NOM_MUN'].astype(str).apply(normalize_text)

    # --- Agregación ---
    logging.info("Agrupando y calculando estadísticas...")
    
    agg_df = df_filtered.groupby(['NOM_ENT', 'NOM_MUN']).agg(
        TOTAL_REGISTROS=('BP1_1', 'count'),
        TOTAL_SEGUROS=('BP1_1', lambda x: (x == 1).sum()),
        TOTAL_INSEGUROS=('BP1_1', lambda x: (x == 2).sum()),
        TOTAL_NO_RESPONDE=('BP1_1', lambda x: (x == 9).sum())
    ).reset_index()

    # --- Cálculo de Porcentajes ---
    agg_df['PCT_SEGUROS'] = (agg_df['TOTAL_SEGUROS'] / agg_df['TOTAL_REGISTROS'] * 100).round(2)
    agg_df['PCT_INSEGUROS'] = (agg_df['TOTAL_INSEGUROS'] / agg_df['TOTAL_REGISTROS'] * 100).round(2)
    agg_df['PCT_NO_RESPONDE'] = (agg_df['TOTAL_NO_RESPONDE'] / agg_df['TOTAL_REGISTROS'] * 100).round(2)

    # --- Composición Final ---
    agg_df['AÑO'] = year
    agg_df['TRIMESTRE'] = quarter
    
    final_cols = [
        'NOM_ENT', 'NOM_MUN', 'TOTAL_REGISTROS', 'TOTAL_SEGUROS', 
        'TOTAL_INSEGUROS', 'TOTAL_NO_RESPONDE', 'PCT_SEGUROS', 
        'PCT_INSEGUROS', 'PCT_NO_RESPONDE', 'AÑO', 'TRIMESTRE'
    ]
    agg_df = agg_df[final_cols]

    # --- Guardado ---
    output_filename = f"procesado_{year}_{quarter}t_cb.csv"
    output_path = os.path.join(OUTPUT_DIR, output_filename)
    
    try:
        agg_df.to_csv(output_path, index=False, encoding='utf-8')
        logging.info(f"Archivo procesado y guardado exitosamente en: {output_path}")
    except Exception as e:
        logging.error(f"No se pudo guardar el archivo de salida {output_path}. Error: {e}")

def main():
    """Función principal para orquestar el procesamiento."""
    logging.info("=================================================")
    logging.info("INICIO DEL SCRIPT DE PROCESAMIENTO DE DATOS ENSU")
    logging.info("=================================================")
    
    search_pattern = os.path.join(INPUT_DATA_DIR, "**", "*cb*.csv")
    file_list = glob.glob(search_pattern, recursive=True)

    if not file_list:
        logging.warning(f"No se encontraron archivos con el patrón '{os.path.basename(search_pattern)}' en {INPUT_DATA_DIR}.")
    else:
        logging.info(f"Se encontraron {len(file_list)} archivos para procesar.")
        for file_path in file_list:
            process_dataset(file_path)
            
    logging.info("=================================================")
    logging.info("FIN DEL SCRIPT DE PROCESAMIENTO")
    logging.info("=================================================")

if __name__ == "__main__":
    main()