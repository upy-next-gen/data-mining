#!/usr/bin/env python3
"""
Fase 1: Mapeo y Descubrimiento (Versión Definitiva)
- Busca solo archivos de datos principales, ignorando catálogos.
- Estandariza siempre el trimestre a 1, 2, 3 o 4.
"""
import os
import json
import logging
import re
from datetime import datetime
from pathlib import Path

def setup_logging():
    os.makedirs("logs", exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(f"logs/fase1_mapeo_{datetime.now().strftime('%Y%m%d')}.log", mode='w', encoding='utf-8'),
            logging.StreamHandler()
        ]
    )

def extraer_periodo(ruta):
    """Extrae el año y SIEMPRE devuelve el trimestre (1-4)."""
    ruta_lower = str(ruta).lower()
    # Regla para formato _YYYY_Qt (ej: _2019_1t)
    match = re.search(r'_(20\d{2})_([1-4])t', ruta_lower)
    if match: return int(match.group(1)), int(match.group(2))
    # Regla para formato _QQ_YYYY (ej: _01_2024), donde QQ es el trimestre
    match = re.search(r'_(\d{1,2})_(\d{4})', ruta_lower)
    if match: return int(match.group(2)), int(match.group(1))
    # Regla para formato ensuMMYY (ej: ensu0322), donde MM es el mes
    match = re.search(r'ensu[a-z_]*?(\d{2})(\d{2})', ruta_lower)
    if match:
        month, year_short = int(match.group(1)), int(match.group(2))
        return 2000 + year_short, (month - 1) // 3 + 1
    logging.error(f"No se pudo extraer periodo para la ruta: {ruta}")
    return None, None

def mapear_archivos(data_dir="data", temp_dir="temp"):
    """Busca archivos CSV de la ENSU cuyo NOMBRE coincida con los patrones."""
    logging.info("Iniciando Fase 1: Mapeo Selectivo.")
    os.makedirs(temp_dir, exist_ok=True)
    archivos_encontrados = []
    patrones_busqueda = ['_cb_', 'conjunto_de_datos_cb', 'ensu_cb']

    for root, _, files in os.walk(data_dir):
        for filename in files:
            if filename.lower().endswith('.csv') and any(p in filename.lower() for p in patrones_busqueda):
                filepath = Path(root) / filename
                try:
                    año, trimestre = extraer_periodo(str(filepath))
                    if año is None: continue
                    metadata = {
                        'filepath': str(filepath),
                        'filename': filename,
                        'año': año,
                        'trimestre': trimestre,
                        'periodo_str': f"{año}_Q{trimestre}"
                    }
                    archivos_encontrados.append(metadata)
                    logging.info(f"Archivo de datos principal encontrado: {filename}")
                except Exception as e:
                    logging.error(f"Error al procesar el archivo {filepath}: {e}")

    archivos_encontrados.sort(key=lambda x: x['periodo_str'])
    output_data = {
        'timestamp': datetime.now().isoformat(),
        'archivos': archivos_encontrados
    }
    output_json_path = Path(temp_dir) / 'mapeo_archivos.json'
    with open(output_json_path, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=4, ensure_ascii=False)
    logging.info(f"Mapeo de {len(archivos_encontrados)} archivos guardado en: {output_json_path}")

if __name__ == '__main__':
    setup_logging()
    mapear_archivos()
    logging.info("Fase 1 completada.")