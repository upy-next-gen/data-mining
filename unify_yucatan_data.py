#!/usr/bin/env python3
"""
Script para unificar todos los archivos procesados de inseguridad de Yucatán
Consolida los datos en un archivo único ordenado cronológicamente
"""

import os
import pandas as pd
import logging
from pathlib import Path
from typing import List, Tuple

def setup_logging():
    """Configura el sistema de logging para la unificación"""
    log_file = "data/yucatan-inseguridad/unificacion_yucatan.log"
    
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
    logger.info("INICIANDO UNIFICACIÓN DE DATOS DE INSEGURIDAD - YUCATÁN")
    logger.info("="*80)
    return logger

def find_processed_files(data_dir: str) -> List[str]:
    """
    Encuentra todos los archivos CSV procesados
    """
    logger = logging.getLogger(__name__)
    
    pattern = "procesado_*_cb.csv"
    processed_files = []
    
    for file in os.listdir(data_dir):
        if file.startswith("procesado_") and file.endswith("_cb.csv"):
            full_path = os.path.join(data_dir, file)
            processed_files.append(full_path)
            logger.info(f"Archivo encontrado: {file}")
    
    logger.info(f"Total de archivos procesados encontrados: {len(processed_files)}")
    return processed_files

def extract_year_quarter_from_filename(filename: str) -> Tuple[int, int]:
    """
    Extrae año y trimestre del nombre del archivo
    Formato esperado: procesado_YYYY_QQT_cb.csv
    """
    logger = logging.getLogger(__name__)
    
    try:
        # Remover path y extensión
        basename = os.path.basename(filename)
        # procesado_2024_03T_cb.csv -> ['procesado', '2024', '03T', 'cb', 'csv']
        parts = basename.replace('.csv', '').split('_')
        
        year = int(parts[1])
        quarter_str = parts[2].replace('T', '')
        quarter = int(quarter_str)
        
        logger.debug(f"Extraído de {filename}: año={year}, trimestre={quarter}")
        return year, quarter
        
    except Exception as e:
        logger.error(f"Error extrayendo año/trimestre de {filename}: {str(e)}")
        return 0, 0

def load_and_validate_file(file_path: str) -> pd.DataFrame:
    """
    Carga un archivo CSV y valida su estructura
    """
    logger = logging.getLogger(__name__)
    
    try:
        df = pd.read_csv(file_path, encoding='utf-8')
        logger.info(f"Archivo cargado: {os.path.basename(file_path)} - {len(df)} registros")
        
        # Validar columnas esperadas
        expected_columns = [
            'NOM_ENT', 'NOM_MUN', 'TOTAL_REGISTROS', 'TOTAL_SEGUROS', 
            'TOTAL_INSEGUROS', 'TOTAL_NO_RESPONDE', 'PCT_SEGUROS', 
            'PCT_INSEGUROS', 'PCT_NO_RESPONDE', 'ANO', 'TRIMESTRE'
        ]
        
        missing_columns = [col for col in expected_columns if col not in df.columns]
        if missing_columns:
            logger.warning(f"Columnas faltantes en {file_path}: {missing_columns}")
        
        extra_columns = [col for col in df.columns if col not in expected_columns]
        if extra_columns:
            logger.warning(f"Columnas adicionales en {file_path}: {extra_columns}")
        
        return df
        
    except Exception as e:
        logger.error(f"Error cargando {file_path}: {str(e)}")
        return pd.DataFrame()

def detect_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Detecta y reporta registros duplicados
    """
    logger = logging.getLogger(__name__)
    
    # Definir qué columnas constituyen un registro único
    unique_columns = ['NOM_ENT', 'NOM_MUN', 'ANO', 'TRIMESTRE']
    
    # Encontrar duplicados
    duplicates = df[df.duplicated(subset=unique_columns, keep=False)]
    
    if len(duplicates) > 0:
        logger.warning(f"Se encontraron {len(duplicates)} registros duplicados:")
        
        for _, row in duplicates.iterrows():
            logger.warning(f"  Duplicado: {row['NOM_MUN']} - {row['ANO']}Q{row['TRIMESTRE']}")
        
        # Mantener solo el primer registro de cada duplicado
        df_clean = df.drop_duplicates(subset=unique_columns, keep='first')
        removed_count = len(df) - len(df_clean)
        
        if removed_count > 0:
            logger.info(f"Duplicados removidos: {removed_count}")
        
        return df_clean
    else:
        logger.info("No se encontraron registros duplicados")
        return df

def sort_data_chronologically(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ordena los datos cronológicamente (año, trimestre, municipio)
    """
    logger = logging.getLogger(__name__)
    
    # Ordenar por año, trimestre y municipio
    df_sorted = df.sort_values(['ANO', 'TRIMESTRE', 'NOM_MUN'], ascending=[True, True, True])
    
    logger.info("Datos ordenados cronológicamente (más antiguos primero)")
    
    return df_sorted.reset_index(drop=True)

def generate_summary_stats(df: pd.DataFrame) -> dict:
    """
    Genera estadísticas de resumen de los datos unificados
    """
    logger = logging.getLogger(__name__)
    
    stats = {
        'total_registros': len(df),
        'municipios_unicos': df['NOM_MUN'].nunique(),
        'municipios_lista': sorted(df['NOM_MUN'].unique().tolist()),
        'año_minimo': df['ANO'].min(),
        'año_maximo': df['ANO'].max(),
        'trimestre_minimo': df['TRIMESTRE'].min(),
        'trimestre_maximo': df['TRIMESTRE'].max(),
        'total_encuestas': df['TOTAL_REGISTROS'].sum(),
        'promedio_seguridad': df['PCT_SEGUROS'].mean(),
        'promedio_inseguridad': df['PCT_INSEGUROS'].mean(),
        'registros_por_año': df.groupby('ANO').size().to_dict(),
        'registros_por_municipio': df.groupby('NOM_MUN').size().to_dict()
    }
    
    logger.info("Estadísticas de resumen generadas")
    return stats

def save_unified_data(df: pd.DataFrame, output_path: str):
    """
    Guarda los datos unificados en un archivo CSV
    """
    logger = logging.getLogger(__name__)
    
    try:
        df.to_csv(output_path, index=False, encoding='utf-8')
        logger.info(f"Datos unificados guardados en: {output_path}")
        logger.info(f"Total de registros guardados: {len(df)}")
    except Exception as e:
        logger.error(f"Error guardando archivo unificado: {str(e)}")
        raise

def main():
    """
    Función principal de unificación
    """
    logger = setup_logging()
    
    # Configuración
    data_dir = "data/yucatan-inseguridad"
    output_filename = "data-yucatan-inseguridad.csv"
    output_path = os.path.join(data_dir, output_filename)
    
    try:
        # 1. Encontrar archivos procesados
        processed_files = find_processed_files(data_dir)
        
        if not processed_files:
            logger.error("No se encontraron archivos procesados para unificar")
            return
        
        # 2. Cargar y combinar todos los archivos
        all_dataframes = []
        files_loaded = 0
        
        for file_path in processed_files:
            df = load_and_validate_file(file_path)
            if not df.empty:
                all_dataframes.append(df)
                files_loaded += 1
            else:
                logger.warning(f"Archivo omitido por errores: {file_path}")
        
        if not all_dataframes:
            logger.error("No se pudieron cargar archivos válidos")
            return
        
        logger.info(f"Archivos cargados exitosamente: {files_loaded}")
        
        # 3. Combinar todos los DataFrames
        logger.info("Combinando todos los archivos...")
        unified_df = pd.concat(all_dataframes, ignore_index=True)
        logger.info(f"Datos combinados: {len(unified_df)} registros totales")
        
        # 4. Detectar y manejar duplicados
        unified_df = detect_duplicates(unified_df)
        
        # 5. Ordenar cronológicamente
        unified_df = sort_data_chronologically(unified_df)
        
        # 6. Generar estadísticas de resumen
        stats = generate_summary_stats(unified_df)
        
        # 7. Guardar archivo unificado
        save_unified_data(unified_df, output_path)
        
        # 8. Reporte final
        logger.info("="*80)
        logger.info("RESUMEN FINAL DE LA UNIFICACIÓN")
        logger.info("="*80)
        logger.info(f"Archivos procesados: {files_loaded}")
        logger.info(f"Total de registros: {stats['total_registros']}")
        logger.info(f"Municipios únicos: {stats['municipios_unicos']}")
        logger.info(f"Municipios: {', '.join(stats['municipios_lista'])}")
        logger.info(f"Período: {stats['año_minimo']}Q{stats['trimestre_minimo']} - {stats['año_maximo']}Q{stats['trimestre_maximo']}")
        logger.info(f"Total de encuestas: {stats['total_encuestas']:,}")
        logger.info(f"Promedio de seguridad: {stats['promedio_seguridad']:.2f}%")
        logger.info(f"Promedio de inseguridad: {stats['promedio_inseguridad']:.2f}%")
        logger.info(f"Archivo unificado: {output_path}")
        
        # Detalle por año
        logger.info("\nRegistros por año:")
        for año, count in sorted(stats['registros_por_año'].items()):
            logger.info(f"  {año}: {count} registros")
        
        # Detalle por municipio
        logger.info("\nRegistros por municipio:")
        for municipio, count in sorted(stats['registros_por_municipio'].items()):
            logger.info(f"  {municipio}: {count} registros")
        
        logger.info("="*80)
        logger.info("UNIFICACIÓN COMPLETADA EXITOSAMENTE")
        logger.info("="*80)
        
    except Exception as e:
        logger.error(f"Error crítico en la unificación: {str(e)}")
        raise

if __name__ == "__main__":
    main()
