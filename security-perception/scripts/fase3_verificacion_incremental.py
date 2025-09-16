#!/usr/bin/env python3
"""
Fase 3: Verificación Incremental (Versión Definitiva)
"""
import json
import logging
import os
from datetime import datetime

def setup_logging():
    os.makedirs("logs", exist_ok=True)
    logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s',
                        handlers=[logging.FileHandler(f"logs/fase3_verificacion_{datetime.now().strftime('%Y%m%d')}.log"),
                                  logging.StreamHandler()])

def load_validated_files(filepath="temp/archivos_validados.json"):
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)

def scan_processed_files(processed_dir="data/yucatan_processed"):
    if not os.path.exists(processed_dir):
        return set()
    return {f.replace("yucatan_security_", "").replace(".csv", "") for f in os.listdir(processed_dir) if f.endswith('.csv')}

def main():
    setup_logging()
    logger = logging.getLogger(__name__)
    logger.info("=== INICIANDO FASE 3: VERIFICACIÓN INCREMENTAL ===")
    
    validated_data = load_validated_files()
    processed_periods = scan_processed_files()
    pending_files = []
    
    for archivo in validated_data.get('archivos', []):
        if archivo.get('es_procesable') and archivo.get('periodo_str') not in processed_periods:
            pending_files.append(archivo)

    output_data = {'timestamp': datetime.now().isoformat(), 'archivos_pendientes': pending_files}
    with open("temp/archivos_pendientes.json", 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=4, ensure_ascii=False)
    logger.info(f"Fase 3 completada. Se encontraron {len(pending_files)} archivos pendientes.")

if __name__ == "__main__":
    main()