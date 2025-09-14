#!/usr/bin/env python3
"""
Fase 2: Validación de archivos ENSU
"""

import json
import logging
import pandas as pd
from datetime import datetime
import os

def setup_logging():
    """Configurar sistema de logging"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = f"logs/fase2_validacion_{timestamp}.log"
    
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

def load_mapping(filepath="temp/mapeo_archivos.json"):
    """Cargar mapeo de archivos de Fase 1"""
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)

def validate_columns(filepath, required_columns):
    """Validar que el archivo tenga las columnas requeridas"""
    try:
        # Leer 5 filas para validación (estandarizado)
        df = pd.read_csv(filepath, nrows=5)
        columnas_presentes = set(df.columns)
        columnas_requeridas = set(required_columns)
        
        columnas_faltantes = columnas_requeridas - columnas_presentes
        
        return {
            'columnas_presentes': True if not columnas_faltantes else False,
            'columnas_faltantes': list(columnas_faltantes),
            'columnas_totales': len(df.columns)
        }
    except Exception as e:
        return {
            'columnas_presentes': False,
            'error': str(e)
        }

def validate_bp1_1_values(filepath, sample_size=1000):
    """Validar valores en columna BP1_1"""
    try:
        df = pd.read_csv(filepath, usecols=['BP1_1'], nrows=sample_size)
        
        # Convertir a numérico
        df['BP1_1'] = pd.to_numeric(df['BP1_1'], errors='coerce')
        
        # Valores únicos
        valores_unicos = df['BP1_1'].dropna().unique()
        valores_validos = [1, 2, 9]
        valores_invalidos = [v for v in valores_unicos if v not in valores_validos]
        
        # Conteo de valores
        value_counts = df['BP1_1'].value_counts().to_dict()
        
        # Calcular NULLs
        total_rows = len(df)
        null_count = df['BP1_1'].isna().sum()
        null_percentage = (null_count / total_rows * 100) if total_rows > 0 else 0
        
        return {
            'valores_validos': len(valores_invalidos) == 0,
            'valores_unicos': sorted(valores_unicos.tolist()),
            'valores_invalidos': valores_invalidos,
            'distribucion': value_counts,
            'nulls_count': int(null_count),
            'nulls_percentage': round(null_percentage, 2),
            'muestra_size': total_rows
        }
    except Exception as e:
        return {
            'valores_validos': False,
            'error': str(e)
        }

def check_yucatan_data(filepath):
    """Verificar si hay datos de Yucatán"""
    try:
        df = pd.read_csv(filepath, usecols=['NOM_ENT'])
        
        # Buscar Yucatán (con y sin tilde)
        yucatan_mask = df['NOM_ENT'].str.upper().isin(['YUCATAN', 'YUCATÁN'])
        yucatan_count = yucatan_mask.sum()
        
        # Estados únicos
        estados_unicos = df['NOM_ENT'].nunique()
        
        return {
            'tiene_yucatan': bool(yucatan_count > 0),
            'registros_yucatan': int(yucatan_count),
            'total_registros': len(df),
            'porcentaje_yucatan': round((yucatan_count / len(df) * 100), 2) if len(df) > 0 else 0,
            'estados_unicos': int(estados_unicos)
        }
    except Exception as e:
        return {
            'tiene_yucatan': False,
            'error': str(e)
        }

def validate_file(archivo_metadata):
    """Validación completa de un archivo"""
    logger = logging.getLogger(__name__)
    filepath = archivo_metadata['filepath']
    
    logger.info(f"Validando: {filepath}")
    
    # Columnas requeridas
    required_columns = ['NOM_ENT', 'NOM_MUN', 'NOM_CD', 'BP1_1']
    
    # Realizar validaciones
    validacion = {
        'filepath': filepath,
        'periodo_str': archivo_metadata['periodo_str'],
        'validacion_columnas': validate_columns(filepath, required_columns),
        'validacion_bp1_1': validate_bp1_1_values(filepath),
        'validacion_yucatan': check_yucatan_data(filepath)
    }
    
    # Determinar si es procesable
    es_procesable = (
        validacion['validacion_columnas'].get('columnas_presentes', False) and
        validacion['validacion_bp1_1'].get('valores_validos', False) and
        validacion['validacion_yucatan'].get('tiene_yucatan', False)
    )
    
    validacion['es_procesable'] = es_procesable
    
    # Log de resultado
    if es_procesable:
        logger.info(f"  [PROCESABLE] - {archivo_metadata['periodo_str']}")
    else:
        logger.warning(f"  [NO PROCESABLE] - {archivo_metadata['periodo_str']}")
        if not validacion['validacion_columnas'].get('columnas_presentes', False):
            logger.warning(f"    Columnas faltantes: {validacion['validacion_columnas'].get('columnas_faltantes', [])}")
        if not validacion['validacion_bp1_1'].get('valores_validos', False):
            logger.warning(f"    Valores inválidos en BP1_1: {validacion['validacion_bp1_1'].get('valores_invalidos', [])}")
        if not validacion['validacion_yucatan'].get('tiene_yucatan', False):
            logger.warning(f"    No contiene datos de Yucatán")
    
    return validacion

def detect_and_resolve_duplicates(validaciones):
    """Detectar y resolver archivos duplicados por período"""
    logger = logging.getLogger(__name__)
    
    periodos = {}
    for val in validaciones:
        if val['es_procesable']:
            periodo = val['periodo_str']
            if periodo not in periodos:
                periodos[periodo] = []
            periodos[periodo].append(val)
    
    duplicados_info = {}
    archivos_seleccionados = {}
    
    for periodo, vals in periodos.items():
        if len(vals) > 1:
            logger.warning(f"=== DUPLICADOS DETECTADOS PARA {periodo} ===")
            logger.warning(f"  Se encontraron {len(vals)} archivos:")
            
            # Analizar cada archivo duplicado
            for val in vals:
                filepath = val['filepath']
                try:
                    file_stats = os.stat(filepath)
                    size_mb = file_stats.st_size / (1024 * 1024)
                    mod_time = datetime.fromtimestamp(file_stats.st_mtime)
                    
                    # Contar registros de Yucatán
                    yuc_count = val['validacion_yucatan'].get('registros_yucatan', 0)
                    
                    logger.warning(f"    - {filepath}")
                    logger.warning(f"      Tamaño: {size_mb:.2f} MB")
                    logger.warning(f"      Modificado: {mod_time.isoformat()}")
                    logger.warning(f"      Registros Yucatán: {yuc_count}")
                    
                    val['_file_size_mb'] = size_mb
                    val['_modified_time'] = mod_time
                    val['_yucatan_count'] = yuc_count
                except Exception as e:
                    logger.error(f"      Error obteniendo stats: {e}")
                    val['_file_size_mb'] = 0
                    val['_modified_time'] = datetime.min
                    val['_yucatan_count'] = 0
            
            # Seleccionar el mejor archivo (más reciente con más datos de Yucatán)
            mejor_archivo = max(vals, key=lambda x: (x['_yucatan_count'], x['_modified_time']))
            archivos_seleccionados[periodo] = mejor_archivo['filepath']
            
            logger.info(f"  ✓ Archivo seleccionado: {mejor_archivo['filepath']}")
            logger.info(f"    Razón: {mejor_archivo['_yucatan_count']} registros de Yucatán, modificado {mejor_archivo['_modified_time'].isoformat()}")
            
            # Marcar los otros como duplicados
            for val in vals:
                if val['filepath'] != mejor_archivo['filepath']:
                    val['es_procesable'] = False
                    val['razon_no_procesable'] = f'Duplicado - Se seleccionó {os.path.basename(mejor_archivo["filepath"])}'
            
            duplicados_info[periodo] = {
                'archivos': [v['filepath'] for v in vals],
                'seleccionado': mejor_archivo['filepath'],
                'criterio': f'{mejor_archivo["_yucatan_count"]} registros Yucatán'
            }
    
    return duplicados_info, archivos_seleccionados

def save_validation(validaciones, output_path="temp/archivos_validados.json"):
    """Guardar resultados de validación"""
    logger = logging.getLogger(__name__)
    
    # Asegurar que el directorio temp existe
    os.makedirs("temp", exist_ok=True)
    
    # Detectar y resolver duplicados
    duplicados_info, archivos_seleccionados = detect_and_resolve_duplicates(validaciones)
    
    output_data = {
        'timestamp': datetime.now().isoformat(),
        'total_archivos': len(validaciones),
        'procesables': sum(1 for v in validaciones if v['es_procesable']),
        'no_procesables': sum(1 for v in validaciones if not v['es_procesable']),
        'duplicados_info': duplicados_info,
        'archivos_seleccionados': archivos_seleccionados,
        'validaciones': validaciones
    }
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    
    logger.info(f"=== RESUMEN DE VALIDACIÓN ===")
    logger.info(f"Total archivos analizados: {output_data['total_archivos']}")
    logger.info(f"Archivos procesables: {output_data['procesables']}")
    logger.info(f"Archivos no procesables: {output_data['no_procesables']}")
    logger.info(f"Períodos con duplicados resueltos: {len(duplicados_info)}")
    
    if duplicados_info:
        logger.info("Duplicados resueltos:")
        for periodo, info in duplicados_info.items():
            logger.info(f"  {periodo}: {len(info['archivos'])} archivos, seleccionado por {info['criterio']}")

def main():
    logger = setup_logging()
    
    try:
        logger.info("=== INICIANDO FASE 2: VALIDACIÓN ===")
        
        # Cargar mapeo de Fase 1
        mapeo = load_mapping()
        
        # Validar cada archivo
        validaciones = []
        for archivo in mapeo['archivos']:
            validacion = validate_file(archivo)
            validaciones.append(validacion)
        
        # Guardar resultados
        save_validation(validaciones)
        
        logger.info("=== FASE 2 COMPLETADA EXITOSAMENTE ===")
        
    except Exception as e:
        logger.error(f"Error fatal en Fase 2: {e}")
        raise

if __name__ == "__main__":
    main()
