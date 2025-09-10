#!/usr/bin/env python3
"""
Fase 1: Descubrimiento y Mapeo de Archivos
Busca archivos CB en la carpeta data y genera un inventario JSON
"""

import os
import re
import json
import logging
from pathlib import Path
from datetime import datetime

# Configurar logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/fase1_mapeo.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def buscar_archivos_cb(ruta_base='data'):
    """
    Busca archivos que contengan el patrón CB en su ruta
    """
    archivos_encontrados = []
    patron_cb = re.compile(r'(_cb_|conjunto_de_datos_cb|ensu_cb)', re.IGNORECASE)
    
    logger.info(f"Iniciando búsqueda en: {ruta_base}")
    
    for root, dirs, files in os.walk(ruta_base):
        for file in files:
            if file.endswith('.csv'):
                ruta_completa = os.path.join(root, file)
                if patron_cb.search(ruta_completa):
                    info_archivo = {
                        'ruta': ruta_completa,
                        'nombre': file,
                        'tamano': os.path.getsize(ruta_completa),
                        'fecha_modificacion': datetime.fromtimestamp(
                            os.path.getmtime(ruta_completa)
                        ).isoformat()
                    }
                    archivos_encontrados.append(info_archivo)
                    logger.debug(f"Archivo CB encontrado: {ruta_completa}")
    
    logger.info(f"Total de archivos CB encontrados: {len(archivos_encontrados)}")
    return archivos_encontrados

def extraer_trimestre(ruta):
    """
    Intenta extraer información del trimestre del archivo
    """
    # Buscar patrones como '2024_1t', '2025_2t', etc.
    patron_trimestre = re.search(r'(\d{4})_(\d)t', ruta)
    if patron_trimestre:
        return {
            'año': int(patron_trimestre.group(1)),
            'trimestre': int(patron_trimestre.group(2))
        }
    
    # Buscar patrones como '0624' (junio 2024 = 2t 2024)
    patron_fecha = re.search(r'_(\d{2})(\d{2})', ruta)
    if patron_fecha:
        mes = int(patron_fecha.group(1))
        año = 2000 + int(patron_fecha.group(2))
        trimestre = (mes - 1) // 3 + 1
        return {
            'año': año,
            'trimestre': trimestre
        }
    
    return None

def main():
    """
    Función principal de la Fase 1
    """
    logger.info("=== FASE 1: DESCUBRIMIENTO Y MAPEO ===")
    
    # Crear carpetas necesarias
    os.makedirs('logs', exist_ok=True)
    os.makedirs('temp', exist_ok=True)
    
    # Buscar archivos
    archivos = buscar_archivos_cb()
    
    # Enriquecer con información de trimestre
    for archivo in archivos:
        info_trimestre = extraer_trimestre(archivo['ruta'])
        if info_trimestre:
            archivo['trimestre_info'] = info_trimestre
            logger.debug(f"Trimestre identificado para {archivo['nombre']}: {info_trimestre}")
        else:
            logger.warning(f"No se pudo identificar trimestre para: {archivo['nombre']}")
    
    # Guardar inventario
    inventario = {
        'fecha_generacion': datetime.now().isoformat(),
        'total_archivos': len(archivos),
        'archivos': archivos
    }
    
    ruta_inventario = 'temp/inventario_archivos_cb.json'
    with open(ruta_inventario, 'w', encoding='utf-8') as f:
        json.dump(inventario, f, indent=2, ensure_ascii=False)
    
    logger.info(f"Inventario guardado en: {ruta_inventario}")
    
    # Resumen
    logger.info("=== RESUMEN FASE 1 ===")
    logger.info(f"Archivos CB encontrados: {len(archivos)}")
    if archivos:
        logger.info("Primeros 5 archivos:")
        for archivo in archivos[:5]:
            logger.info(f"  - {archivo['nombre']}")
    
    return len(archivos) > 0

if __name__ == "__main__":
    exito = main()
    exit(0 if exito else 1)