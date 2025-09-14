#!/usr/bin/env python3
"""
Fase 4: Procesamiento de datos ENSU para Yucatán (Versión Robusta)
"""

import json
import logging
import pandas as pd
import os
from datetime import datetime

def setup_logging():
    """Configurar sistema de logging"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = f"logs/fase4_procesamiento_{timestamp}.log"
    
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

def clean_text(series):
    """Función de limpieza universal para columnas de texto."""
    if series is None:
        return None
    # Convertir a string, quitar espacios/comillas, poner en mayúsculas y normalizar caracteres.
    return series.astype(str).str.strip().str.strip('"').str.upper().str.replace('Ñ', 'N').str.replace('YUCATÁN', 'YUCATAN')

def load_pending_files(filepath="temp/archivos_pendientes.json"):
    """Cargar archivos pendientes de Fase 3"""
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)

def process_single_file(file_info):
    """Procesar un archivo individual con lógica de normalización robusta."""
    logger = logging.getLogger(__name__)
    filepath = file_info['filepath']
    periodo = file_info['periodo_str']
    
    logger.info(f"{ '='*60}")
    logger.info(f"PROCESANDO ARCHIVO: {os.path.basename(filepath)}")
    logger.info(f"Período: {periodo}")
    
    try:
        # --- Lógica de Lectura con Fallback de Codificación ---
        try:
            df = pd.read_csv(filepath, encoding='utf-8', low_memory=False)
            logger.info("Archivo leído exitosamente con codificación UTF-8.")
        except UnicodeDecodeError:
            df = pd.read_csv(filepath, encoding='latin-1', low_memory=False)
            logger.info("Fallback a latin-1: Archivo leído exitosamente.")

        # --- Lógica de Filtrado Robusta para Yucatán ---
        logger.info("Iniciando normalización y filtrado para Yucatán...")
        df_yucatan = pd.DataFrame()

        # Opción 1: Usar CVE_ENT (código numérico)
        if 'CVE_ENT' in df.columns:
            df['CVE_ENT_CLEAN'] = pd.to_numeric(clean_text(df['CVE_ENT']), errors='coerce')
            df_yucatan = df[df['CVE_ENT_CLEAN'] == 31].copy()
            logger.info(f"Filtrado por CVE_ENT=31. Se encontraron {len(df_yucatan)} registros.")

        # Opción 2: Usar NOM_ENT (nombre) si el primer método falla o no encuentra nada
        if df_yucatan.empty and 'NOM_ENT' in df.columns:
            logger.info("CVE_ENT no encontró datos o no existe. Usando NOM_ENT como alternativa.")
            df['NOM_ENT_CLEAN'] = clean_text(df['NOM_ENT'])
            df_yucatan = df[df['NOM_ENT_CLEAN'].str.contains('YUCATAN', na=False)].copy()
            logger.info(f"Filtrado por NOM_ENT='YUCATAN'. Se encontraron {len(df_yucatan)} registros.")

        if df_yucatan.empty:
            logger.warning("No se encontraron registros para Yucatán en este archivo.")
            return None

        # --- Normalización de Columnas de Municipio y Ciudad ---
        logger.info("Normalizando columnas de municipio y ciudad...")
        df_yucatan['NOM_MUN'] = clean_text(df_yucatan['NOM_MUN'])
        if 'NOM_CD' in df_yucatan.columns:
            df_yucatan['NOM_CD'] = clean_text(df_yucatan['NOM_CD'])
        else:
            df_yucatan['NOM_CD'] = "SIN_CIUDAD"

        # --- Agregación de Datos ---
        logger.info("Agrupando datos y calculando estadísticas...")
        required_cols = ['NOM_ENT', 'NOM_MUN', 'NOM_CD', 'BP1_1']
        df_yucatan = df_yucatan[required_cols].copy()
        df_yucatan['BP1_1'] = pd.to_numeric(df_yucatan['BP1_1'], errors='coerce')
        df_yucatan.dropna(subset=['BP1_1'], inplace=True)

        grouped_data = []
        for (nom_ent, nom_mun, nom_cd), group in df_yucatan.groupby(['NOM_ENT', 'NOM_MUN', 'NOM_CD']):
            total_respuestas = len(group)
            total_seguros = int((group['BP1_1'] == 1).sum())
            total_inseguros = int((group['BP1_1'] == 2).sum())
            total_no_responde = int((group['BP1_1'] == 9).sum())
            
            grouped_data.append({
                'NOM_ENT': nom_ent,
                'NOM_MUN': nom_mun,
                'NOM_CD': nom_cd,
                'TOTAL_SEGUROS': total_seguros,
                'TOTAL_INSEGUROS': total_inseguros,
                'TOTAL_NO_RESPONDE': total_no_responde,
                'TOTAL_RESPUESTAS': total_respuestas
            })
        
        df_resultado = pd.DataFrame(grouped_data)
        
        if df_resultado.empty:
            logger.warning("La agrupación no produjo resultados.")
            return None

        df_resultado['PORCENTAJE_SEGUROS'] = (df_resultado['TOTAL_SEGUROS'] / df_resultado['TOTAL_RESPUESTAS'] * 100).round(2)
        df_resultado['PORCENTAJE_INSEGUROS'] = (df_resultado['TOTAL_INSEGUROS'] / df_resultado['TOTAL_RESPUESTAS'] * 100).round(2)
        df_resultado['PORCENTAJE_NO_RESPONDE'] = (df_resultado['TOTAL_NO_RESPONDE'] / df_resultado['TOTAL_RESPUESTAS'] * 100).round(2)
        df_resultado['PERIODO'] = periodo
        df_resultado['FECHA_PROCESAMIENTO'] = datetime.now().isoformat()
        
        logger.info(f"Procesamiento del período {periodo} completado. {len(df_resultado)} registros agregados.")

        return {
            'dataframe': df_resultado,
            'estadisticas': {
                'periodo': periodo,
                'registros_yucatan': len(df_yucatan),
                'municipios': int(df_resultado['NOM_MUN'].nunique()),
            }
        }

    except Exception as e:
        logger.error(f"Error fatal procesando archivo {filepath}: {e}", exc_info=True)
        return None

def save_processed_file(result, periodo, output_dir="data/yucatan_processed"):
    """Guardar archivo procesado con el nuevo formato de nombre."""
    logger = logging.getLogger(__name__)
    os.makedirs(output_dir, exist_ok=True)
    
    year, quarter = periodo.split('_Q')
    output_filename = f"procesado_{year}_Q{quarter}_cb.csv"
    output_path = os.path.join(output_dir, output_filename)
    
    result['dataframe'].to_csv(output_path, index=False, encoding='utf-8')
    logger.info(f"Archivo procesado guardado en: {output_path}")
    return output_path

def save_processing_summary(results, output_path="temp/procesamiento_resultados.json"):
    """Guardar resumen del procesamiento."""
    logger = logging.getLogger(__name__)
    os.makedirs("temp", exist_ok=True)
    
    summary = {
        'timestamp': datetime.now().isoformat(),
        'archivos_procesados': len([r for r in results if r['exito']]),
        'archivos_fallidos': len([r for r in results if not r['exito']]),
        'resultados': results
    }
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    logger.info(f"Resumen de procesamiento guardado en: {output_path}")

def main():
    logger = setup_logging()
    
    try:
        logger.info("=== INICIANDO FASE 4: PROCESAMIENTO (VERSIÓN ROBUSTA) ===")
        pending_data = load_pending_files()
        
        if pending_data['total_pendientes'] == 0:
            logger.info("No hay archivos pendientes de procesar.")
            return
        
        results = []
        for file_info in pending_data['archivos_pendientes']:
            result = process_single_file(file_info)
            
            if result:
                output_path = save_processed_file(result, file_info['periodo_str'])
                results.append({
                    'periodo': file_info['periodo_str'],
                    'archivo_origen': file_info['filepath'],
                    'archivo_salida': output_path,
                    'exito': True,
                    'estadisticas': result['estadisticas']
                })
            else:
                results.append({
                    'periodo': file_info['periodo_str'],
                    'archivo_origen': file_info['filepath'],
                    'exito': False,
                    'error': 'Error en procesamiento'
                })
        
        save_processing_summary(results)
        logger.info("\n=== FASE 4 COMPLETADA EXITOSAMENTE ===")
        
    except Exception as e:
        logger.error(f"Error fatal en Fase 4: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main()