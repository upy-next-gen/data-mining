#!/usr/bin/env python3
"""
Fase 5: Consolida todos los datos procesados en un único dataset maestro.
"""
import pandas as pd
import logging
from datetime import datetime
import os
from glob import glob

# --- CONFIGURACIÓN ---
PROCESSED_DATA_PATH = "data/yucatan_processed/*.csv"
REPORTS_DIR = "reports"
DATASET_MAESTRO_PATH = os.path.join(REPORTS_DIR, "dataset_final_yucatan.csv")
LOG_PATH = os.path.join("logs", f"fase5_consolidacion_{datetime.now().strftime('%Y%m%d')}.log")

def setup_logging():
    os.makedirs("logs", exist_ok=True)
    logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s',
                        handlers=[logging.FileHandler(LOG_PATH, mode='w', encoding='utf-8'),
                                  logging.StreamHandler()])

def load_and_create_master_dataset(path):
    """Carga, consolida, ordena y guarda el dataset maestro."""
    logger = logging.getLogger(__name__)
    logger.info("Iniciando consolidación de datos para el dataset maestro...")
    
    all_files = glob(path)
    if not all_files:
        logger.error("No se encontraron archivos procesados en 'data/yucatan_processed/'. Ejecuta la Fase 4 primero.")
        return False

    df_list = [pd.read_csv(f, encoding='utf-8') for f in all_files]
    df_consolidado = pd.concat(df_list, ignore_index=True)
    
    # Ordenar el DataFrame por Año y luego por Trimestre
    df_sorted = df_consolidado.sort_values(by=['AÑO', 'TRIMESTRE'], ascending=True)
    
    os.makedirs(REPORTS_DIR, exist_ok=True)
    df_sorted.to_csv(DATASET_MAESTRO_PATH, index=False, encoding='utf-8')
    
    logger.info(f"Se consolidaron y ordenaron {len(all_files)} archivos.")
    logger.info(f"Dataset maestro guardado exitosamente en: {DATASET_MAESTRO_PATH}")
    return True

def main():
    setup_logging()
    load_and_create_master_dataset(PROCESSED_DATA_PATH)
    logging.info(" FASE 5: CONSOLIDACIÓN COMPLETADA ")

if __name__ == "__main__":
    main()