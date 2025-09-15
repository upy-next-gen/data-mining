#!/usr/bin/env python3
"""
Script para procesar datos de inseguridad de Yucatán de la ENSU
Extrae y procesa archivos CB de la ENSU para generar estadísticas de percepción de seguridad
"""

import os
import pandas as pd
import logging
import re
from pathlib import Path
from typing import List, Tuple, Optional, Dict
import unicodedata

# Configuración de logging
def setup_logging():
    """Configura el sistema de logging con formato detallado"""
    log_file = "data/yucatan-inseguridad/procesamiento_yucatan.log"
    
    # Crear directorio si no existe
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    
    # Configurar formato detallado
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    
    logger = logging.getLogger(__name__)
    logger.info("="*80)
    logger.info("INICIANDO PROCESAMIENTO DE DATOS DE INSEGURIDAD - YUCATÁN")
    logger.info("="*80)
    return logger

def normalize_text(text: str) -> str:
    """
    Normaliza texto removiendo acentos y convirtiendo Ñ a N
    """
    if not isinstance(text, str):
        return str(text)
    
    # Normalizar unicode y remover acentos
    normalized = unicodedata.normalize('NFD', text)
    without_accents = ''.join(c for c in normalized if unicodedata.category(c) != 'Mn')
    
    # Convertir Ñ a N (mayúsculas y minúsculas)
    result = without_accents.replace('Ñ', 'N').replace('ñ', 'n')
    
    return result

def extract_year_quarter(file_path: str) -> Tuple[Optional[int], Optional[int]]:
    """
    Extrae año y trimestre del nombre del archivo
    Formato esperado: yyyy-tt
    """
    logger = logging.getLogger(__name__)
    
    # Patrones posibles basados en los nombres observados
    patterns = [
        # Patrón: conjunto_de_datos_ensu_02_2017_csv
        r'ensu_(\d{2})_(\d{4})',
        # Patrón: conjunto_de_datos_ensu_2018_3t_csv
        r'ensu_(\d{4})_(\d{1})t',
        # Patrón: conjunto_de_datos_ensu_cb_0325 (mmyy)
        r'cb_(\d{2})(\d{2})$',
        # Patrón: conjunto_de_datos_ENSU_2019_1t_csv
        r'ENSU_(\d{4})_(\d{1})t',
        # Patrón: conjunto_de_datos_cb_ENSU_2019_2t
        r'cb_ENSU_(\d{4})_(\d{1})t',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, file_path)
        if match:
            groups = match.groups()
            
            if 'cb_' in pattern and pattern.endswith('$'):  # Formato mmyy
                month, year_suffix = groups
                year = 2000 + int(year_suffix)
                quarter = (int(month) - 1) // 3 + 1
                logger.info(f"Extraído de patrón mmyy: {file_path} -> {year}-{quarter:02d}")
                return year, quarter
            
            elif len(groups[0]) == 2:  # Primer grupo es trimestre (02)
                quarter, year = int(groups[0]), int(groups[1])
                logger.info(f"Extraído trimestre-año: {file_path} -> {year}-{quarter:02d}")
                return year, quarter
            
            else:  # Primer grupo es año (2018)
                year, quarter = int(groups[0]), int(groups[1])
                logger.info(f"Extraído año-trimestre: {file_path} -> {year}-{quarter:02d}")
                return year, quarter
    
    logger.warning(f"No se pudo extraer año/trimestre de: {file_path}")
    return None, None

def find_cb_datasets(raw_data_path: str) -> List[str]:
    """
    Encuentra todos los archivos CSV con 'cb' en rutas /conjunto_de_datos/
    Excluye archivos de diccionario
    """
    logger = logging.getLogger(__name__)
    cb_files = []
    
    logger.info(f"Buscando archivos CB en: {raw_data_path}")
    
    for root, dirs, files in os.walk(raw_data_path):
        # Solo procesar si estamos en una carpeta conjunto_de_datos y NO es diccionario
        if ('conjunto_de_datos' in root and 
            not '__MACOSX' in root and 
            'diccionario_de_datos' not in root):
            for file in files:
                if 'cb' in file.lower() and file.endswith('.csv'):
                    full_path = os.path.join(root, file)
                    cb_files.append(full_path)
                    logger.info(f"Archivo CB encontrado: {full_path}")
    
    logger.info(f"Total de archivos CB encontrados: {len(cb_files)}")
    return cb_files

def validate_required_columns(df: pd.DataFrame, file_path: str) -> bool:
    """
    Valida que el dataset contenga las columnas requeridas
    """
    logger = logging.getLogger(__name__)
    required_columns = ['NOM_ENT', 'NOM_MUN', 'BP1_1']
    
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        logger.error(f"Columnas faltantes en {file_path}: {missing_columns}")
        logger.error(f"Columnas disponibles: {list(df.columns)}")
        return False
    
    logger.info(f"Validación exitosa para {file_path} - todas las columnas requeridas presentes")
    return True

def filter_yucatan_data(df: pd.DataFrame, file_path: str) -> pd.DataFrame:
    """
    Filtra datos de Yucatán considerando variaciones del nombre
    """
    logger = logging.getLogger(__name__)
    
    # Limpiar caracteres de control como \r
    df['NOM_ENT'] = df['NOM_ENT'].astype(str).str.strip()
    df['NOM_MUN'] = df['NOM_MUN'].astype(str).str.strip()
    
    # Filtrar usando texto normalizado
    df['NOM_ENT_NORMALIZED'] = df['NOM_ENT'].apply(normalize_text)
    yucatan_mask = df['NOM_ENT_NORMALIZED'].str.upper() == 'YUCATAN'
    
    yucatan_data = df[yucatan_mask].copy()
    
    if len(yucatan_data) == 0:
        logger.warning(f"No se encontraron datos de Yucatán en {file_path}")
        logger.info(f"Entidades disponibles: {df['NOM_ENT'].unique()}")
    else:
        logger.info(f"Datos de Yucatán encontrados: {len(yucatan_data)} registros en {file_path}")
        logger.info(f"Variaciones encontradas: {df[yucatan_mask]['NOM_ENT'].unique()}")
    
    return yucatan_data

def process_security_perception(df: pd.DataFrame, year: int, quarter: int, file_path: str) -> pd.DataFrame:
    """
    Procesa los datos de percepción de seguridad
    """
    logger = logging.getLogger(__name__)
    
    # Normalizar NOM_ENT y NOM_MUN
    df['NOM_ENT'] = 'Yucatan'  # Normalizar a formato estándar
    df['NOM_MUN'] = df['NOM_MUN'].astype(str).str.strip().apply(normalize_text)
    
    # Validar valores de BP1_1
    valid_values = [1, 2, 9]
    invalid_mask = ~df['BP1_1'].isin(valid_values)
    
    if invalid_mask.any():
        invalid_count = invalid_mask.sum()
        invalid_values = df[invalid_mask]['BP1_1'].unique()
        logger.warning(f"Valores inválidos en BP1_1 encontrados: {invalid_values} ({invalid_count} registros)")
        logger.warning(f"Se tratarán como 'No responde' (9)")
        
        # Convertir valores inválidos a 9 (No responde)
        df.loc[invalid_mask, 'BP1_1'] = 9
    
    # Agrupar por municipio y calcular estadísticas
    grouped = df.groupby('NOM_MUN').agg({
        'BP1_1': ['count', lambda x: (x == 1).sum(), lambda x: (x == 2).sum(), lambda x: (x == 9).sum()]
    }).round(2)
    
    # Aplanar nombres de columnas
    grouped.columns = ['TOTAL_REGISTROS', 'TOTAL_SEGUROS', 'TOTAL_INSEGUROS', 'TOTAL_NO_RESPONDE']
    
    # Calcular porcentajes
    grouped['PCT_SEGUROS'] = ((grouped['TOTAL_SEGUROS'] / grouped['TOTAL_REGISTROS']) * 100).round(2)
    grouped['PCT_INSEGUROS'] = ((grouped['TOTAL_INSEGUROS'] / grouped['TOTAL_REGISTROS']) * 100).round(2)
    grouped['PCT_NO_RESPONDE'] = ((grouped['TOTAL_NO_RESPONDE'] / grouped['TOTAL_REGISTROS']) * 100).round(2)
    
    # Validar que los porcentajes sumen 100 (con tolerancia de 0.1 por redondeo)
    total_pct = grouped['PCT_SEGUROS'] + grouped['PCT_INSEGUROS'] + grouped['PCT_NO_RESPONDE']
    invalid_pct = abs(total_pct - 100) > 0.1
    
    if invalid_pct.any():
        logger.warning(f"Porcentajes no suman 100% para algunos municipios en {file_path}")
        logger.warning(f"Municipios afectados: {grouped[invalid_pct].index.tolist()}")
    
    # Agregar metadatos
    grouped['NOM_ENT'] = 'Yucatan'
    grouped['ANO'] = year
    grouped['TRIMESTRE'] = quarter
    
    # Resetear index para que NOM_MUN sea una columna
    result = grouped.reset_index()
    
    # Reordenar columnas
    columns_order = [
        'NOM_ENT', 'NOM_MUN', 'TOTAL_REGISTROS', 'TOTAL_SEGUROS', 
        'TOTAL_INSEGUROS', 'TOTAL_NO_RESPONDE', 'PCT_SEGUROS', 
        'PCT_INSEGUROS', 'PCT_NO_RESPONDE', 'ANO', 'TRIMESTRE'
    ]
    
    result = result[columns_order]
    
    logger.info(f"Procesamiento completado para {file_path}")
    logger.info(f"Municipios procesados: {len(result)}")
    logger.info(f"Total de registros: {result['TOTAL_REGISTROS'].sum()}")
    
    return result

def save_processed_data(df: pd.DataFrame, year: int, quarter: int, output_dir: str):
    """
    Guarda los datos procesados en formato CSV
    """
    logger = logging.getLogger(__name__)
    
    filename = f"procesado_{year}_{quarter:02d}T_cb.csv"
    output_path = os.path.join(output_dir, filename)
    
    try:
        df.to_csv(output_path, index=False, encoding='utf-8')
        logger.info(f"Archivo guardado exitosamente: {output_path}")
        logger.info(f"Registros guardados: {len(df)}")
    except Exception as e:
        logger.error(f"Error al guardar {output_path}: {str(e)}")
        raise

def main():
    """
    Función principal del procesamiento
    """
    logger = setup_logging()
    
    # Configuración de rutas
    raw_data_path = "raw data"
    output_dir = "data/yucatan-inseguridad"
    
    try:
        # 1. Encontrar archivos CB
        cb_files = find_cb_datasets(raw_data_path)
        
        if not cb_files:
            logger.error("No se encontraron archivos CB para procesar")
            return
        
        processed_count = 0
        skipped_count = 0
        
        # 2. Procesar cada archivo
        for file_path in cb_files:
            logger.info(f"Procesando: {file_path}")
            
            try:
                # Extraer año y trimestre
                year, quarter = extract_year_quarter(file_path)
                if year is None or quarter is None:
                    logger.error(f"No se pudo extraer año/trimestre de {file_path}")
                    skipped_count += 1
                    continue
                
                # Leer archivo CSV
                logger.info(f"Leyendo archivo: {file_path}")
                try:
                    df = pd.read_csv(file_path, encoding='utf-8', low_memory=False)
                except UnicodeDecodeError:
                    # Intentar con latin-1 si UTF-8 falla
                    logger.warning(f"Error UTF-8 en {file_path}, intentando con latin-1")
                    df = pd.read_csv(file_path, encoding='latin-1', low_memory=False)
                
                logger.info(f"Archivo leído: {len(df)} registros, {len(df.columns)} columnas")
                
                # Validar columnas requeridas
                if not validate_required_columns(df, file_path):
                    skipped_count += 1
                    continue
                
                # Filtrar datos de Yucatán
                yucatan_data = filter_yucatan_data(df, file_path)
                if len(yucatan_data) == 0:
                    skipped_count += 1
                    continue
                
                # Procesar datos de percepción de seguridad
                processed_data = process_security_perception(yucatan_data, year, quarter, file_path)
                
                # Guardar datos procesados
                save_processed_data(processed_data, year, quarter, output_dir)
                
                processed_count += 1
                logger.info(f"Archivo procesado exitosamente: {file_path}")
                
            except Exception as e:
                logger.error(f"Error procesando {file_path}: {str(e)}")
                skipped_count += 1
                continue
        
        # Resumen final
        logger.info("="*80)
        logger.info("RESUMEN FINAL DEL PROCESAMIENTO")
        logger.info("="*80)
        logger.info(f"Archivos encontrados: {len(cb_files)}")
        logger.info(f"Archivos procesados exitosamente: {processed_count}")
        logger.info(f"Archivos omitidos: {skipped_count}")
        logger.info(f"Archivos de salida generados en: {output_dir}")
        
    except Exception as e:
        logger.error(f"Error crítico en el procesamiento: {str(e)}")
        raise

if __name__ == "__main__":
    main()
