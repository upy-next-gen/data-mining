
import os
import json
import logging
import re
from datetime import datetime

# --- Configuration ---
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
TEMP_DIR = os.path.join(PROJECT_ROOT, 'temp')
LOG_DIR = os.path.join(PROJECT_ROOT, 'logs')
PROCESSED_DIR = os.path.join(PROJECT_ROOT, 'data', 'yucatan_processed')

INPUT_JSON = os.path.join(TEMP_DIR, 'archivos_validados.json')
OUTPUT_JSON = os.path.join(TEMP_DIR, 'archivos_pendientes.json')

# Regex to extract period from processed filenames
PROCESSED_FILE_REGEX = re.compile(r"yucatan_security_(\d{4}_Q\d)\.csv", re.IGNORECASE)

# --- Setup ---
def setup_environment():
    """Create necessary directories and configure logging."""
    os.makedirs(TEMP_DIR, exist_ok=True)
    os.makedirs(LOG_DIR, exist_ok=True)
    os.makedirs(PROCESSED_DIR, exist_ok=True) # Ensure processed dir exists

    log_filename = f"fase3_verificacion_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    log_filepath = os.path.join(LOG_DIR, log_filename)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filepath),
            logging.StreamHandler()
        ]
    )
    logging.info("--- Iniciando Fase 3: Verificación Incremental ---")
    return logging.getLogger()

def find_processed_files(logger):
    """Scans the processed directory to find already processed periods."""
    processed_files = {}
    logger.info(f"Escaneando directorio de procesados: {PROCESSED_DIR}")
    try:
        for filename in os.listdir(PROCESSED_DIR):
            match = PROCESSED_FILE_REGEX.match(filename)
            if match:
                periodo = match.group(1).replace('_', '-') # Normalize to YYYY-QN
                filepath = os.path.join(PROCESSED_DIR, filename)
                mod_time = datetime.fromtimestamp(os.path.getmtime(filepath)).isoformat()
                processed_files[periodo] = {
                    "filepath": filepath,
                    "fecha_procesamiento": mod_time
                }
                logger.info(f"Encontrado archivo procesado para el periodo {periodo}")
    except FileNotFoundError:
        logger.warning(f"El directorio de procesados no existe: {PROCESSED_DIR}. Se asumirá que no hay archivos procesados.")
    except Exception as e:
        logger.error(f"Error escaneando el directorio de procesados: {e}")
    
    return processed_files

# --- Main Logic ---
def main():
    logger = setup_environment()

    try:
        with open(INPUT_JSON, 'r', encoding='utf-8') as f:
            validated_data = json.load(f)
        logger.info(f"Cargado exitosamente {INPUT_JSON}")
    except FileNotFoundError:
        logger.error(f"Error: No se encontró el archivo de entrada {INPUT_JSON}. Ejecute la Fase 2 primero.")
        return

    processed_files = find_processed_files(logger)
    
    archivos_pendientes = []
    archivos_saltados = []

    candidate_files = validated_data.get('archivos_seleccionados', {})
    if not candidate_files:
        logger.warning("No se encontraron 'archivos_seleccionados' en el JSON de validación.")
        return

    logger.info(f"Verificando {len(candidate_files)} archivos seleccionados contra los ya procesados.")

    for periodo, original_filepath in candidate_files.items():
        # Normalize period from YYYY-QN to YYYY-QN for comparison
        periodo_norm = periodo.replace('_', '-')

        if periodo_norm in processed_files:
            processed_info = processed_files[periodo_norm]
            archivos_saltados.append({
                "periodo": periodo_norm,
                "archivo_original": original_filepath,
                "archivo_procesado": processed_info['filepath'],
                "fecha_procesamiento": processed_info['fecha_procesamiento']
            })
            logger.info(f"Saltando periodo {periodo_norm}, ya fue procesado.")
        else:
            archivos_pendientes.append({
                "filepath": original_filepath,
                "periodo_str": periodo_norm
            })
            logger.info(f"Periodo {periodo_norm} está pendiente de procesamiento.")

    # --- Final Output ---
    final_output = {
        "timestamp": datetime.now().isoformat(),
        "total_pendientes": len(archivos_pendientes),
        "total_saltados": len(archivos_saltados),
        "archivos_pendientes": sorted(archivos_pendientes, key=lambda x: x['periodo_str']),
        "archivos_saltados": sorted(archivos_saltados, key=lambda x: x['periodo'])
    }

    try:
        with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
            json.dump(final_output, f, indent=4, ensure_ascii=False)
        logger.info(f"Resultados de verificación guardados en: {OUTPUT_JSON}")
    except Exception as e:
        logger.error(f"No se pudo escribir el archivo JSON de salida: {e}")

    logger.info(f"--- Fase 3 Finalizada: {len(archivos_pendientes)} pendientes, {len(archivos_saltados)} saltados ---")

if __name__ == "__main__":
    main()
