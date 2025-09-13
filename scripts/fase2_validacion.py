#!/usr/bin/env python3
"""
Fase 2: Validación de archivos ENSU
"""

import json
import logging
import pandas as pd
from datetime import datetime
import os
import numpy as np

class NumpyJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, np.bool_):
            return bool(obj)
        return super(NumpyJSONEncoder, self).default(obj)

def setup_logging():
    """Configurar sistema de logging"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = f"logs/fase2_validacion_{timestamp}.log"
    
    os.makedirs("logs", exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] %(levelname)s: %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8')
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
        df = pd.read_csv(filepath, nrows=5)
        columnas_presentes = set(df.columns)
        columnas_requeridas = set(required_columns)
        
        columnas_faltantes = columnas_requeridas - columnas_presentes
        
        return {
            'columnas_presentes': bool(not columnas_faltantes),
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
        df['BP1_1'] = pd.to_numeric(df['BP1_1'], errors='coerce')
        
        valores_unicos = df['BP1_1'].dropna().unique()
        valores_validos = [1, 2, 9]
        valores_invalidos = [v for v in valores_unicos if v not in valores_validos]
        
        value_counts = {str(k): int(v) for k, v in df['BP1_1'].value_counts().to_dict().items()}
        
        total_rows = len(df)
        null_count = df['BP1_1'].isna().sum()
        null_percentage = (null_count / total_rows * 100) if total_rows > 0 else 0
        
        return {
            'valores_validos': bool(len(valores_invalidos) == 0),
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
    """Verificar si hay datos de Yucatán, usando NOM_ENT o CVE_ENT."""
    try:
        # Intentar leer las columnas requeridas, manejando el caso de que no existan
        cols_to_read = []
        temp_df = pd.read_csv(filepath, nrows=0, encoding='latin-1') # Leer solo cabeceras para chequear columnas
        if 'NOM_ENT' in temp_df.columns:
            cols_to_read.append('NOM_ENT')
        if 'CVE_ENT' in temp_df.columns:
            cols_to_read.append('CVE_ENT')

        if not cols_to_read:
            # Si no existe ni NOM_ENT ni CVE_ENT, no podemos determinar la entidad.
            return {'tiene_yucatan': False, 'error': 'No se encontraron columnas de entidad (NOM_ENT o CVE_ENT).'}

        df = pd.read_csv(filepath, usecols=cols_to_read, encoding='latin-1')
        
        yucatan_mask = pd.Series([False] * len(df)) # Iniciar con todo en False

        # Estrategia 1: Usar NOM_ENT si existe
        if 'NOM_ENT' in df.columns:
            # Limpiar la columna NOM_ENT para ser robustos
            nom_ent_clean = df['NOM_ENT'].astype(str).str.strip().str.upper()
            yucatan_mask = yucatan_mask | nom_ent_clean.str.contains('YUCAT', na=False)

        # Estrategia 2: Usar CVE_ENT si existe
        if 'CVE_ENT' in df.columns:
            # El código para Yucatán es 31
            cve_ent_clean = pd.to_numeric(df['CVE_ENT'], errors='coerce')
            yucatan_mask = yucatan_mask | (cve_ent_clean == 31)

        yucatan_count = yucatan_mask.sum()
        
        # Para la estadística de estados únicos, preferimos NOM_ENT si existe
        estados_unicos = 0
        if 'NOM_ENT' in df.columns:
            estados_unicos = df['NOM_ENT'].nunique()
        elif 'CVE_ENT' in df.columns:
            estados_unicos = df['CVE_ENT'].nunique()

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
    required_columns = ['NOM_ENT', 'NOM_MUN', 'NOM_CD', 'BP1_1']
    
    validacion = {
        'filepath': filepath,
        'periodo_str': archivo_metadata['periodo_str'],
        'validacion_columnas': validate_columns(filepath, required_columns),
        'validacion_bp1_1': validate_bp1_1_values(filepath),
        'validacion_yucatan': check_yucatan_data(filepath)
    }
    
    es_procesable = (
        validacion['validacion_columnas'].get('columnas_presentes', False) and
        validacion['validacion_bp1_1'].get('valores_validos', False) and
        validacion['validacion_yucatan'].get('tiene_yucatan', False)
    )
    
    validacion['es_procesable'] = bool(es_procesable)
    
    if es_procesable:
        logger.info(f"  [OK] PROCESABLE - {archivo_metadata['periodo_str']}")
    else:
        logger.warning(f"  [X] NO PROCESABLE - {archivo_metadata['periodo_str']}")
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
            for val in vals:
                filepath = val['filepath']
                try:
                    file_stats = os.stat(filepath)
                    size_mb = file_stats.st_size / (1024 * 1024)
                    mod_time = datetime.fromtimestamp(file_stats.st_mtime)
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
            
            mejor_archivo = max(vals, key=lambda x: (x['_yucatan_count'], x['_modified_time']))
            archivos_seleccionados[periodo] = mejor_archivo['filepath']
            logger.info(f"  [OK] Archivo seleccionado: {mejor_archivo['filepath']}")
            logger.info(f"    Razón: {mejor_archivo['_yucatan_count']} registros de Yucatán, modificado {mejor_archivo['_modified_time'].isoformat()}")
            
            for val in vals:
                if val['filepath'] != mejor_archivo['filepath']:
                    val['es_procesable'] = False
                    val['razon_no_procesable'] = f'Duplicado - Se seleccionó {os.path.basename(mejor_archivo["filepath"])}'
            
            duplicados_info[periodo] = {
                'archivos': [v['filepath'] for v in vals],
                'seleccionado': mejor_archivo['filepath'],
                'criterio': f"{mejor_archivo['_yucatan_count']} registros Yucatán"
            }
    
    return duplicados_info, archivos_seleccionados

def save_validation(validaciones, output_path="temp/archivos_validados.json"):
    """Guardar resultados de validación"""
    logger = logging.getLogger(__name__)
    os.makedirs("temp", exist_ok=True)
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
        json.dump(output_data, f, indent=2, ensure_ascii=False, cls=NumpyJSONEncoder)
    
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
        mapeo = load_mapping()
        validaciones = []
        for archivo in mapeo['archivos']:
            validacion = validate_file(archivo)
            validaciones.append(validacion)
        save_validation(validaciones)
        logger.info("=== FASE 2 COMPLETADA EXITOSAMENTE ===")
    except Exception as e:
        logger.error(f"Error fatal en Fase 2: {e}")
        raise

if __name__ == "__main__":
    main()