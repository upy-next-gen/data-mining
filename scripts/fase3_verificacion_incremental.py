#!/usr/bin/env python3
"""
Fase 3: Verificación Incremental
"""

import json
import logging
import os
from datetime import datetime
from pathlib import Path

def setup_logging():
    """Configurar sistema de logging"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = f"logs/fase3_verificacion_{timestamp}.log"
    
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

def load_validated_files(filepath="temp/archivos_validados.json"):
    """Cargar archivos validados de Fase 2"""
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)

def scan_processed_files(processed_dir="data/yucatan_processed"):
    """Escanear archivos ya procesados"""
    logger = logging.getLogger(__name__)
    
    processed_files = {}
    
    if not os.path.exists(processed_dir):
        logger.info(f"Directorio {processed_dir} no existe. Se asume que no hay archivos procesados.")
        return processed_files
    
    for file in os.listdir(processed_dir):
        if file.endswith('.csv'):
            # Extraer período del nombre del archivo
            # Formato esperado: yucatan_security_YYYY_QN.csv
            parts = file.replace('.csv', '').split('_')
            if len(parts) >= 4:
                try:
                    year = parts[-2]
                    quarter = parts[-1]
                    periodo_str = f"{year}_{quarter}"
                    
                    file_path = os.path.join(processed_dir, file)
                    file_stats = os.stat(file_path)
                    
                    processed_files[periodo_str] = {
                        'filename': file,
                        'filepath': file_path,
                        'size_mb': round(file_stats.st_size / (1024 * 1024), 2),
                        'modified_date': datetime.fromtimestamp(file_stats.st_mtime).isoformat()
                    }
                    
                    logger.info(f"Archivo procesado encontrado: {file} ({periodo_str})")
                except Exception as e:
                    logger.warning(f"No se pudo parsear período de {file}: {e}")
    
    return processed_files

def identify_pending_files(validated_data, processed_files):
    """Identificar archivos pendientes de procesar"""
    logger = logging.getLogger(__name__)
    
    pending = []
    skipped = []
    
    for validacion in validated_data['validaciones']:
        if not validacion['es_procesable']:
            # Los archivos con período UNKNOWN_XXX no se procesan automáticamente
            # Requieren revisión manual para identificar el período correcto
            if 'UNKNOWN' in validacion.get('periodo_str', ''):
                logger.warning(f"SALTANDO archivo con período no identificado: {validacion['periodo_str']}")
            continue
        
        periodo = validacion['periodo_str']
        
        if periodo in processed_files:
            logger.info(f"SALTANDO {periodo}: Ya procesado ({processed_files[periodo]['filename']})")
            skipped.append({
                'periodo': periodo,
                'archivo_original': validacion['filepath'],
                'archivo_procesado': processed_files[periodo]['filepath'],
                'fecha_procesamiento': processed_files[periodo]['modified_date']
            })
        else:
            logger.info(f"PENDIENTE {periodo}: Será procesado")
            pending.append(validacion)
    
    return pending, skipped

def save_pending_files(pending, skipped, output_path="temp/archivos_pendientes.json"):
    """Guardar lista de archivos pendientes"""
    logger = logging.getLogger(__name__)
    
    # Asegurar que el directorio temp existe
    os.makedirs("temp", exist_ok=True)
    
    output_data = {
        'timestamp': datetime.now().isoformat(),
        'total_pendientes': len(pending),
        'total_saltados': len(skipped),
        'archivos_pendientes': pending,
        'archivos_saltados': skipped
    }
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    
    logger.info(f"=== RESUMEN DE VERIFICACIÓN INCREMENTAL ===")
    logger.info(f"Archivos pendientes de procesar: {len(pending)}")
    logger.info(f"Archivos ya procesados (saltados): {len(skipped)}")
    
    if pending:
        logger.info("Períodos a procesar:")
        for p in pending:
            logger.info(f"  - {p['periodo_str']}")

def main():
    logger = setup_logging()
    
    try:
        logger.info("=== INICIANDO FASE 3: VERIFICACIÓN INCREMENTAL ===")
        
        # Cargar archivos validados
        validated_data = load_validated_files()
        
        # Escanear archivos ya procesados
        processed_files = scan_processed_files()
        
        # Identificar pendientes
        pending, skipped = identify_pending_files(validated_data, processed_files)
        
        # Guardar resultados
        save_pending_files(pending, skipped)
        
        logger.info("=== FASE 3 COMPLETADA EXITOSAMENTE ===")
        
    except Exception as e:
        logger.error(f"Error fatal en Fase 3: {e}")
        raise

if __name__ == "__main__":
    main()
