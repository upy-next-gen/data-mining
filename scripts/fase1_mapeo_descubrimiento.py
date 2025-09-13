#!/usr/bin/env python3
"""
Fase 1: Mapeo y Descubrimiento de archivos ENSU
"""

import os
import json
import logging
from datetime import datetime
from pathlib import Path
import pandas as pd
import re

def setup_logging():
    """Configurar sistema de logging"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = f"logs/fase1_mapeo_{timestamp}.log"
    
    os.makedirs("logs", exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] %(levelname)s: %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def extract_period_from_filename(filepath):
    """Extraer año y trimestre del nombre del archivo"""
    filename = str(filepath)
    
    # Patrones comunes en nombres de archivo ENSU
    patterns = [
        r'(\d{4})_(\d)t',  # 2025_2t
        r'ensu_(\d{2})(\d{2})',  # ensu_0625 (MMYY)
        r'(\d{4}).*[Tt](\d)',  # 2024T3
    ]
    
    for pattern in patterns:
        match = re.search(pattern, filename)
        if match:
            if len(match.group(1)) == 2:  # Formato MMYY
                month = int(match.group(1))
                year = 2000 + int(match.group(2))
                trimestre = (month - 1) // 3 + 1
            else:
                year = int(match.group(1))
                trimestre = int(match.group(2))
            return year, trimestre
    
    return None, None

def extract_period_from_csv(filepath):
    """Extraer período del contenido del CSV"""
    try:
        # Leer 5 filas para buscar columna de período (estandarizado)
        df = pd.read_csv(filepath, nrows=5, encoding='latin-1')
        
        # Buscar columna PER o similar
        period_columns = ['PER', 'PERIODO', 'periodo', 'per']
        for col in period_columns:
            if col in df.columns and not df[col].isna().all():
                period_value = str(df[col].iloc[0])
                # Formato esperado: MMYY
                if len(period_value) == 4 and period_value.isdigit():
                    month = int(period_value[:2])
                    year = 2000 + int(period_value[2:])
                    trimestre = (month - 1) // 3 + 1
                    return year, trimestre
        
        return None, None
    except Exception as e:
        logging.error(f"Error leyendo {filepath}: {e}")
        return None, None

def scan_directory(base_path):
    """Escanear directorio en busca de archivos ENSU CB"""
    logger = logging.getLogger(__name__)
    logger.info(f"=== INICIANDO ESCANEO DE DIRECTORIO: {base_path} ===")
    logger.info(f"Directorio absoluto: {os.path.abspath(base_path)}")
    
    archivos_encontrados = []
    total_csvs_encontrados = 0
    
    # Patrón más específico para archivos CB
    cb_pattern = re.compile(r'(_cb_|conjunto_de_datos_cb|ensu_cb)', re.IGNORECASE)
    
    for root, dirs, files in os.walk(base_path):
        if '__MACOSX' in dirs:
            dirs.remove('__MACOSX')
        csv_files = [f for f in files if f.endswith('.csv')]
        if csv_files:
            logger.info(f"Directorio {root}: {len(csv_files)} archivos CSV encontrados")
            total_csvs_encontrados += len(csv_files)
        
        for file in files:
            if file.endswith('.csv') and cb_pattern.search(file):
                filepath = Path(root) / file
                logger.info(f"Archivo CB identificado: {filepath}")
                logger.info(f"  - Tamaño: {os.path.getsize(filepath) / (1024*1024):.2f} MB")
                
                # Extraer metadata
                try:
                    # Intentar extraer período del nombre
                    year_from_name, trimestre_from_name = extract_period_from_filename(filepath)
                    identificacion_metodo = 'filename' if year_from_name else None
                    
                    # Si no se encuentra en el nombre, buscar en el contenido
                    year = year_from_name
                    trimestre = trimestre_from_name
                    
                    if year is None:
                        logger.info(f"  - Período no encontrado en nombre, analizando contenido CSV...")
                        year, trimestre = extract_period_from_csv(filepath)
                        if year:
                            identificacion_metodo = 'contenido'
                            logger.info(f"  - Período extraído del contenido: {year} Q{trimestre}")
                        else:
                            identificacion_metodo = 'no_identificado'
                            logger.warning(f"  - [ADVERTENCIA] No se pudo identificar el período")
                    else:
                        logger.info(f"  - Período extraído del nombre: {year} Q{trimestre}")
                    
                    # Obtener información del archivo
                    file_stats = os.stat(filepath)
                    
                    metadata = {
                        'filepath': str(filepath),
                        'filename': file,
                        'size_mb': round(file_stats.st_size / (1024 * 1024), 2),
                        'modified_date': datetime.fromtimestamp(file_stats.st_mtime).isoformat(),
                        'year': year,
                        'trimestre': trimestre,
                        'periodo_str': f"{year}_Q{trimestre}" if year else f"UNKNOWN_{len(archivos_encontrados)+1:03d}",  # NOTA: Períodos no identificados se marcan como UNKNOWN_XXX,
                        'identificacion_metodo': identificacion_metodo
                    }
                    
                    # Verificar columnas básicas
                    try:
                        # Leer 5 filas para verificación básica (estandarizado)
                        df_sample = pd.read_csv(filepath, nrows=5, encoding='latin-1')
                        metadata['columnas_muestra'] = list(df_sample.columns)[:10]
                        metadata['total_columnas'] = len(df_sample.columns)
                        
                        # Verificar columnas requeridas
                        required_cols = ['NOM_ENT', 'NOM_MUN', 'NOM_CD', 'BP1_1']
                        cols_presentes = [col for col in required_cols if col in df_sample.columns]
                        cols_faltantes = [col for col in required_cols if col not in df_sample.columns]
                        
                        metadata['columnas_requeridas_presentes'] = cols_presentes
                        metadata['columnas_requeridas_faltantes'] = cols_faltantes
                        
                        if cols_faltantes:
                            logger.warning(f"  - [ADVERTENCIA] Columnas requeridas faltantes: {cols_faltantes}")
                        else:
                            logger.info(f"  - [OK] Todas las columnas requeridas presentes")
                            
                    except Exception as e:
                        logger.error(f"  - Error al leer columnas: {e}")
                        metadata['columnas_muestra'] = []
                        metadata['total_columnas'] = 0
                        metadata['columnas_requeridas_presentes'] = []
                        metadata['columnas_requeridas_faltantes'] = []
                    
                    archivos_encontrados.append(metadata)
                    logger.info(f"  - Metadata completa para período: {metadata['periodo_str']}")
                    
                except Exception as e:
                    logger.error(f"  - ERROR CRÍTICO procesando {filepath}: {e}")
                    logger.error(f"    Tipo de error: {type(e).__name__}")
                    import traceback
                    logger.error(f"    Stack trace: {traceback.format_exc()}")
    
    logger.info(f"=== RESUMEN DE ESCANEO ===")
    logger.info(f"Total de archivos CSV en el directorio: {total_csvs_encontrados}")
    logger.info(f"Total de archivos CB encontrados: {len(archivos_encontrados)}")
    logger.info(f"Archivos con período identificado: {sum(1 for a in archivos_encontrados if a['identificacion_metodo'] != 'no_identificado')}")
    
    return archivos_encontrados

def save_mapping(archivos, output_path="temp/mapeo_archivos.json"):
    """Guardar mapeo en archivo JSON"""
    logger = logging.getLogger(__name__)
    
    # Asegurar que el directorio temp existe
    os.makedirs("temp", exist_ok=True)
    
    output_data = {
        'timestamp': datetime.now().isoformat(),
        'total_archivos': len(archivos),
        'archivos': archivos
    }
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    
    logger.info(f"Mapeo guardado en: {output_path}")
    logger.info(f"Total de archivos encontrados: {len(archivos)}")
    
    # Resumen por período
    periodos = {}
    for archivo in archivos:
        periodo = archivo['periodo_str']
        if periodo not in periodos:
            periodos[periodo] = []
        periodos[periodo].append(archivo['filename'])
    
    logger.info("=== RESUMEN POR PERÍODO ===")
    for periodo, files in sorted(periodos.items()):
        logger.info(f"  {periodo}: {len(files)} archivo(s)")
        if len(files) > 1:
            logger.warning(f"    ADVERTENCIA: Múltiples archivos para {periodo}")

def main():
    logger = setup_logging()
    
    try:
        # Escanear directorio
        archivos = scan_directory("data")
        
        # Guardar mapeo
        save_mapping(archivos)
        
        logger.info("=== FASE 1 COMPLETADA EXITOSAMENTE ===")
        
    except Exception as e:
        logger.error(f"Error fatal en Fase 1: {e}")
        raise

if __name__ == "__main__":
    main()
