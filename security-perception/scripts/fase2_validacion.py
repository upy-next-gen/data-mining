#!/usr/bin/env python3
"""
Fase 2: Validación (Versión Definitiva)
"""
import json
import logging
import pandas as pd
import os
from datetime import datetime

def setup_logging():
    os.makedirs("logs", exist_ok=True)
    logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s',
                        handlers=[logging.FileHandler(f"logs/fase2_validacion_{datetime.now().strftime('%Y%m%d')}.log"),
                                  logging.StreamHandler()])

def load_mapping(filepath="temp/mapeo_archivos.json"):
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)

def check_file_validity(archivo_meta):
    filepath = archivo_meta.get('filepath')
    try:
        df_sample = pd.read_csv(filepath, nrows=5, encoding='utf-8')
        required_cols = {'BP1_1'}
        # La columna de entidad puede tener varios nombres, se valida en Fase 4.
        # La columna de municipio puede no existir.
        if not required_cols.issubset(df_sample.columns):
            logging.warning(f"SALTANDO: {os.path.basename(filepath)} - Faltan columnas base.")
            return False
        return True
    except Exception as e:
        logging.error(f"Error validando {filepath}: {e}")
        return False

def main():
    setup_logging()
    logger = logging.getLogger(__name__)
    logger.info("=== INICIANDO FASE 2: VALIDACIÓN ===")
    
    mapping_data = load_mapping()
    archivos_validados = []
    
    for archivo in mapping_data.get('archivos', []):
        archivo['es_procesable'] = check_file_validity(archivo)
        archivos_validados.append(archivo)

    output_data = {'timestamp': datetime.now().isoformat(), 'archivos': archivos_validados}
    with open("temp/archivos_validados.json", 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=4, ensure_ascii=False)
    logger.info("Fase 2 completada.")

if __name__ == "__main__":
    main()