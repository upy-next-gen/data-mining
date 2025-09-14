
import os
import json
import logging
import traceback
from datetime import datetime
import pandas as pd

# --- Configuration ---
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
TEMP_DIR = os.path.join(PROJECT_ROOT, 'temp')
LOG_DIR = os.path.join(PROJECT_ROOT, 'logs')
PROCESSED_DIR = os.path.join(PROJECT_ROOT, 'data', 'yucatan_processed')

INPUT_JSON = os.path.join(TEMP_DIR, 'archivos_pendientes.json')
OUTPUT_JSON = os.path.join(TEMP_DIR, 'procesamiento_resultados.json')

FINAL_COLUMNS_ORDER = [
    'NOM_ENT', 'NOM_MUN', 'NOM_CD', 'TOTAL_SEGUROS', 'TOTAL_INSEGUROS',
    'TOTAL_NO_RESPONDE', 'TOTAL_RESPUESTAS', 'PORCENTAJE_SEGUROS',
    'PORCENTAJE_INSEGUROS', 'PORCENTAJE_NO_RESPONDE', 'PERIODO', 'FECHA_PROCESAMIENTO'
]

# --- Setup ---
def setup_environment():
    """Create necessary directories and configure logging."""
    os.makedirs(TEMP_DIR, exist_ok=True)
    os.makedirs(LOG_DIR, exist_ok=True)
    os.makedirs(PROCESSED_DIR, exist_ok=True)

    log_filename = f"fase4_procesamiento_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    log_filepath = os.path.join(LOG_DIR, log_filename)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filepath),
            logging.StreamHandler()
        ]
    )
    logging.info("--- Iniciando Fase 4: Procesamiento de Archivos ---")
    return logging.getLogger()

# --- Processing Function ---
def process_file(file_info, logger):
    """Reads, processes, and saves a single data file."""
    filepath = file_info['filepath']
    periodo_str = file_info['periodo_str']
    logger.info(f"Procesando {os.path.basename(filepath)} para el periodo {periodo_str}")

    try:
        # 1. Read CSV
        df = pd.read_csv(filepath, encoding='latin1', low_memory=False)
        registros_originales = len(df)

        # 2. Handle optional NOM_CD column
        if 'NOM_CD' not in df.columns:
            df['NOM_CD'] = "SIN_CIUDAD"
            logger.warning(f"Columna 'NOM_CD' no encontrada. Se creó con valor por defecto 'SIN_CIUDAD'.")

        # 3. Coerce BP1_1 and filter
        df['BP1_1'] = pd.to_numeric(df['BP1_1'], errors='coerce')
        df.dropna(subset=['BP1_1'], inplace=True)
        df = df[df['BP1_1'].isin([1, 2, 9])].copy()
        df['BP1_1'] = df['BP1_1'].astype(int)

        # 4. Filter by state (Yucatán)
        df['NOM_ENT'] = df['NOM_ENT'].str.strip().str.upper()
        df_yuc = df[df['NOM_ENT'].str.contains('YUCAT', na=False)].copy()
        registros_yucatan = len(df_yuc)

        if registros_yucatan == 0:
            raise ValueError("No se encontraron registros para Yucatán en el archivo.")

        # 5. Group by and aggregate
        df_yuc['SEGURO'] = (df_yuc['BP1_1'] == 1).astype(int)
        df_yuc['INSEGURO'] = (df_yuc['BP1_1'] == 2).astype(int)
        df_yuc['NO_RESPONDE'] = (df_yuc['BP1_1'] == 9).astype(int)

        group_keys = ['NOM_ENT', 'NOM_MUN', 'NOM_CD']
        processed_df = df_yuc.groupby(group_keys).agg(
            TOTAL_SEGUROS=('SEGURO', 'sum'),
            TOTAL_INSEGUROS=('INSEGURO', 'sum'),
            TOTAL_NO_RESPONDE=('NO_RESPONDE', 'sum')
        ).reset_index()

        processed_df['TOTAL_RESPUESTAS'] = processed_df[['TOTAL_SEGUROS', 'TOTAL_INSEGUROS', 'TOTAL_NO_RESPONDE']].sum(axis=1)
        
        processed_df['PORCENTAJE_SEGUROS'] = (processed_df['TOTAL_SEGUROS'] / processed_df['TOTAL_RESPUESTAS'] * 100).round(2).fillna(0)
        processed_df['PORCENTAJE_INSEGUROS'] = (processed_df['TOTAL_INSEGUROS'] / processed_df['TOTAL_RESPUESTAS'] * 100).round(2).fillna(0)
        processed_df['PORCENTAJE_NO_RESPONDE'] = (processed_df['TOTAL_NO_RESPONDE'] / processed_df['TOTAL_RESPUESTAS'] * 100).round(2).fillna(0)

        processed_df['PERIODO'] = periodo_str
        processed_df['FECHA_PROCESAMIENTO'] = datetime.now().isoformat()

        # 6. Save to CSV
        output_filename = f"yucatan_security_{periodo_str.replace('-', '_')}.csv"
        output_path = os.path.join(PROCESSED_DIR, output_filename)
        processed_df = processed_df[FINAL_COLUMNS_ORDER]
        processed_df.to_csv(output_path, index=False, encoding='utf-8')
        logger.info(f"Archivo procesado guardado en: {output_path}")

        # Build statistics for report
        total_respuestas_agg = processed_df['TOTAL_RESPUESTAS'].sum()
        stats = {
            "periodo": periodo_str,
            "registros_originales": registros_originales,
            "registros_yucatan": registros_yucatan,
            "municipios": processed_df['NOM_MUN'].nunique(),
            "ciudades": processed_df['NOM_CD'].nunique(),
            "total_respuestas": int(total_respuestas_agg),
            "porcentaje_seguros_general": round(processed_df['TOTAL_SEGUROS'].sum() / total_respuestas_agg * 100, 2) if total_respuestas_agg > 0 else 0,
            "porcentaje_inseguros_general": round(processed_df['TOTAL_INSEGUROS'].sum() / total_respuestas_agg * 100, 2) if total_respuestas_agg > 0 else 0
        }

        return {"periodo": periodo_str, "archivo_origen": filepath, "archivo_salida": output_path, "exito": True, "estadisticas": stats}

    except Exception as e:
        logger.error(f"Fallo el procesamiento para {os.path.basename(filepath)}: {e}")
        logger.error(traceback.format_exc())
        return {"periodo": periodo_str, "archivo_origen": filepath, "exito": False, "error": str(e)}

# --- Main Logic ---
def main():
    logger = setup_environment()

    try:
        with open(INPUT_JSON, 'r', encoding='utf-8') as f:
            pending_data = json.load(f)
        logger.info(f"Cargado exitosamente {INPUT_JSON}")
    except FileNotFoundError:
        logger.error(f"Error: No se encontró el archivo de entrada {INPUT_JSON}. Ejecute la Fase 3 primero.")
        return

    pending_files = pending_data.get('archivos_pendientes', [])
    if not pending_files:
        logger.info("No hay archivos pendientes de procesar. Finalizando.")
        return

    logger.info(f"Iniciando procesamiento de {len(pending_files)} archivos.")
    
    resultados = [process_file(file_info, logger) for file_info in pending_files]

    # --- Final Output ---
    archivos_procesados = sum(1 for r in resultados if r['exito'])
    archivos_fallidos = len(resultados) - archivos_procesados

    final_output = {
        "timestamp": datetime.now().isoformat(),
        "archivos_procesados": archivos_procesados,
        "archivos_fallidos": archivos_fallidos,
        "resultados": resultados
    }

    try:
        with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
            json.dump(final_output, f, indent=4, ensure_ascii=False)
        logger.info(f"Resultados del procesamiento guardados en: {OUTPUT_JSON}")
    except Exception as e:
        logger.error(f"No se pudo escribir el archivo JSON de resultados: {e}")

    logger.info(f"--- Fase 4 Finalizada: {archivos_procesados} procesados, {archivos_fallidos} fallidos ---")

if __name__ == "__main__":
    main()
