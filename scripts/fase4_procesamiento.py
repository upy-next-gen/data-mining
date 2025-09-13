#!/usr/bin/env python3
"""
Fase 4: Procesamiento de datos ENSU para Yucatán
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
            logging.FileHandler(log_file, encoding='utf-8')
        ]
    )
    return logging.getLogger(__name__)

def load_pending_files(filepath="temp/archivos_pendientes.json"):
    """Cargar archivos pendientes de Fase 3"""
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)

def process_single_file(file_info):
    """Procesar un archivo individual"""
    logger = logging.getLogger(__name__)
    filepath = file_info['filepath']
    periodo = file_info['periodo_str']
    
    logger.info(f"{'='*60}")
    logger.info(f"PROCESANDO ARCHIVO: {os.path.basename(filepath)}")
    logger.info(f"Período: {periodo}")
    logger.info(f"Ruta completa: {filepath}")
    
    try:
        # Verificar que el archivo existe
        if not os.path.exists(filepath):
            logger.error(f"ERROR: El archivo no existe en la ruta especificada")
            return None
            
        file_size_mb = os.path.getsize(filepath) / (1024 * 1024)
        logger.info(f"Tamaño del archivo: {file_size_mb:.2f} MB")
        
        # Leer archivo completo
        logger.info("Leyendo archivo CSV...")
        df = pd.read_csv(filepath)
        logger.info(f"✓ Archivo leído exitosamente")
        logger.info(f"  - Registros totales: {len(df):,}")
        logger.info(f"  - Columnas totales: {len(df.columns)}")
        
        # Verificar columnas requeridas
        columns_required = ['NOM_ENT', 'NOM_MUN', 'NOM_CD', 'BP1_1']
        missing_cols = [col for col in columns_required if col not in df.columns]
        if missing_cols:
            logger.error(f"ERROR: Columnas faltantes: {missing_cols}")
            return None
        
        logger.info(f"[OK] Todas las columnas requeridas presentes")
        
        # Seleccionar columnas y analizar datos iniciales
        df = df[columns_required].copy()
        logger.info(f"Analizando datos iniciales:")
        logger.info(f"  - Valores únicos en NOM_ENT: {df['NOM_ENT'].nunique()}")
        logger.info(f"  - Estados presentes: {', '.join(df['NOM_ENT'].unique()[:5])}..." )
        
        # Analizar BP1_1 antes de conversión
        logger.info("Analizando columna BP1_1:")
        logger.info(f"  - Tipo de datos original: {df['BP1_1'].dtype}")
        logger.info(f"  - Valores únicos (muestra): {df['BP1_1'].value_counts().head().to_dict()}")
        logger.info(f"  - Valores nulos: {df['BP1_1'].isna().sum():,}")
        
        # Convertir BP1_1 a numérico
        df['BP1_1'] = pd.to_numeric(df['BP1_1'], errors='coerce')
        nulls_after_conversion = df['BP1_1'].isna().sum()
        logger.info(f"Conversión a numérico completada:")
        logger.info(f"  - Nuevos nulos después de conversión: {nulls_after_conversion:,}")
        
        # Analizar valores antes del filtrado
        all_values = df['BP1_1'].dropna().unique()
        valid_values = [1, 2, 9]
        invalid_values = [v for v in all_values if v not in valid_values]
        
        if invalid_values:
            logger.warning(f"[!] Valores no estándar encontrados en BP1_1: {invalid_values}")
            value_dist = df['BP1_1'].value_counts().to_dict()
            for val in invalid_values:
                count = value_dist.get(val, 0)
                logger.warning(f"    Valor {val}: {count:,} ({count/len(df)*100:.2f}%)")
        
        # Filtrar solo valores válidos (1, 2, 9)
        valid_mask = df['BP1_1'].isin(valid_values)
        registros_invalidos = (~valid_mask).sum()
        df = df[valid_mask].copy()
        
        logger.info(f"Filtrado por valores válidos de BP1_1:")
        logger.info(f"  - Registros con valores válidos: {len(df):,}")
        logger.info(f"  - Registros descartados: {registros_invalidos:,}")
        logger.info(f"  - Distribución de valores válidos:")
        for val in valid_values:
            count = (df['BP1_1'] == val).sum()
            if count > 0:
                logger.info(f"    BP1_1={val}: {count:,} ({count/len(df)*100:.1f}%)")
        
        # Filtrar por Yucatán
        # IMPORTANTE: El formato de NOM_ENT varía según el año:
        # - 2016-2017: 'Yucatán\r' (con tilde y retorno de carro)
        # - 2018-2021: 'Yucatan\r' o 'Yucatan' (sin tilde, con o sin \r)
        # - 2022+: 'YUCATAN' (mayúsculas sin tilde)
        logger.info("Filtrando registros de Yucatán...")
        # Limpiar espacios, caracteres de retorno y normalizar
        df['NOM_ENT_CLEAN'] = df['NOM_ENT'].str.strip().str.upper()
        df_yucatan = df[df['NOM_ENT_CLEAN'].str.contains('YUCAT', case=False, na=False)].copy()
        
        logger.info(f"Resultados del filtrado por Yucatán:")
        logger.info(f"  - Registros de Yucatán: {len(df_yucatan):,}")
        logger.info(f"  - Porcentaje del total: {len(df_yucatan)/len(df)*100:.2f}%")
        
        if len(df_yucatan) == 0:
            logger.error(f"ERROR: No se encontraron registros de Yucatán")
            return None
        elif len(df_yucatan) < 10:
            logger.warning(f"⚠ ADVERTENCIA: Muy pocos registros de Yucatán ({len(df_yucatan)})")
            logger.warning(f"  Los resultados pueden no ser estadísticamente significativos")
        
        # Análisis detallado de Yucatán
        logger.info(f"Análisis de datos de Yucatán:")
        logger.info(f"  - Municipios únicos: {df_yucatan['NOM_MUN'].nunique()}")
        logger.info(f"  - Ciudades únicas: {df_yucatan['NOM_CD'].nunique()}")
        
        # Distribución de BP1_1 en Yucatán
        logger.info(f"  - Distribución de percepción en Yucatán:")
        for val, label in [(1, 'Seguro'), (2, 'Inseguro'), (9, 'No responde')]:
            count = (df_yucatan['BP1_1'] == val).sum()
            if count > 0:
                logger.info(f"    {label} (BP1_1={val}): {count:,} ({count/len(df_yucatan)*100:.1f}%)")
        
        # Agrupar y calcular estadísticas
        logger.info("Agrupando datos por municipio y ciudad...")
        grouped_data = []
        
        for (nom_ent, nom_mun, nom_cd), group in df_yucatan.groupby(['NOM_ENT', 'NOM_MUN', 'NOM_CD']):
            total_seguros = (group['BP1_1'] == 1).sum()
            total_inseguros = (group['BP1_1'] == 2).sum()
            total_no_responde = (group['BP1_1'] == 9).sum()
            total_respuestas = len(group)
            
            # Log para ciudades con pocos registros
            if total_respuestas < 5:
                logger.warning(f"  ⚠ {nom_mun} - {nom_cd}: Solo {total_respuestas} respuestas")
            
            grouped_data.append({
                'NOM_ENT': nom_ent,
                'NOM_MUN': nom_mun,
                'NOM_CD': nom_cd,
                'TOTAL_SEGUROS': int(total_seguros),
                'TOTAL_INSEGUROS': int(total_inseguros),
                'TOTAL_NO_RESPONDE': int(total_no_responde),
                'TOTAL_RESPUESTAS': int(total_respuestas)
            })
        
        df_resultado = pd.DataFrame(grouped_data)
        logger.info(f"✓ Agrupación completada: {len(df_resultado)} combinaciones municipio-ciudad")
        
        # Calcular porcentajes
        df_resultado['PORCENTAJE_SEGUROS'] = (
            df_resultado['TOTAL_SEGUROS'] / df_resultado['TOTAL_RESPUESTAS'] * 100
        ).round(2)
        df_resultado['PORCENTAJE_INSEGUROS'] = (
            df_resultado['TOTAL_INSEGUROS'] / df_resultado['TOTAL_RESPUESTAS'] * 100
        ).round(2)
        df_resultado['PORCENTAJE_NO_RESPONDE'] = (
            df_resultado['TOTAL_NO_RESPONDE'] / df_resultado['TOTAL_RESPUESTAS'] * 100
        ).round(2)
        
        # Agregar metadatos
        df_resultado['PERIODO'] = periodo
        df_resultado['FECHA_PROCESAMIENTO'] = datetime.now().isoformat()
        
        # Log de resumen por municipio
        logger.info("Resumen por municipio:")
        mun_summary = df_resultado.groupby('NOM_MUN').agg({
            'TOTAL_RESPUESTAS': 'sum',
            'TOTAL_SEGUROS': 'sum',
            'TOTAL_INSEGUROS': 'sum'
        })
        
        for mun, row in mun_summary.iterrows():
            pct_seg = row['TOTAL_SEGUROS'] / row['TOTAL_RESPUESTAS'] * 100
            logger.info(f"  - {mun}: {row['TOTAL_RESPUESTAS']:,} respuestas, {pct_seg:.1f}% seguro")
        
        logger.info(f"Totales finales del procesamiento:")
        logger.info(f"  - Municipios únicos: {df_resultado['NOM_MUN'].nunique()}")
        logger.info(f"  - Ciudades únicas: {df_resultado['NOM_CD'].nunique()}")
        logger.info(f"  - Total de registros agregados: {len(df_resultado)}")
        
        # Estadísticas generales
        total_general = df_resultado['TOTAL_RESPUESTAS'].sum()
        pct_seguros = (df_resultado['TOTAL_SEGUROS'].sum() / total_general * 100)
        pct_inseguros = (df_resultado['TOTAL_INSEGUROS'].sum() / total_general * 100)
        
        logger.info(f"  Percepción general - Seguros: {pct_seguros:.1f}%, Inseguros: {pct_inseguros:.1f}%")
        
        return {
            'dataframe': df_resultado,
            'estadisticas': {
                'periodo': periodo,
                'registros_originales': len(df),
                'registros_yucatan': len(df_yucatan),
                'municipios': int(df_resultado['NOM_MUN'].nunique()),
                'ciudades': int(df_resultado['NOM_CD'].nunique()),
                'total_respuestas': int(total_general),
                'porcentaje_seguros_general': round(pct_seguros, 2),
                'porcentaje_inseguros_general': round(pct_inseguros, 2)
            }
        }
        
    except Exception as e:
        logger.error(f"  Error procesando archivo: {e}")
        return None

def save_processed_file(result, periodo, output_dir="data/yucatan_processed"):
    """Guardar archivo procesado"""
    logger = logging.getLogger(__name__)
    
    os.makedirs(output_dir, exist_ok=True)
    
    # Generar nombre de archivo
    output_filename = f"yucatan_security_{periodo}.csv"
    output_path = os.path.join(output_dir, output_filename)
    
    # Guardar DataFrame
    result['dataframe'].to_csv(output_path, index=False, encoding='utf-8')
    
    logger.info(f"  Archivo guardado: {output_path}")
    
    return output_path

def save_processing_summary(results, output_path="temp/procesamiento_resultados.json"):
    """Guardar resumen del procesamiento"""
    logger = logging.getLogger(__name__)
    
    # Asegurar que el directorio temp existe
    os.makedirs("temp", exist_ok=True)
    
    summary = {
        'timestamp': datetime.now().isoformat(),
        'archivos_procesados': len([r for r in results if r['exito']]),
        'archivos_fallidos': len([r for r in results if not r['exito']]),
        'resultados': results
    }
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    
    logger.info(f"=== RESUMEN DE PROCESAMIENTO ===")
    logger.info(f"Archivos procesados exitosamente: {summary['archivos_procesados']}")
    logger.info(f"Archivos con errores: {summary['archivos_fallidos']}")

def main():
    logger = setup_logging()
    
    try:
        logger.info("=== INICIANDO FASE 4: PROCESAMIENTO ===")
        
        # Cargar archivos pendientes
        pending_data = load_pending_files()
        
        if pending_data['total_pendientes'] == 0:
            logger.info("No hay archivos pendientes de procesar")
            return
        
        # Procesar cada archivo
        results = []
        for file_info in pending_data['archivos_pendientes']:
            logger.info(f"\n--- Procesando {file_info['periodo_str']} ---")
            
            result = process_single_file(file_info)
            
            if result:
                # Guardar archivo procesado
                output_path = save_processed_file(
                    result, 
                    file_info['periodo_str']
                )
                
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
        
        # Guardar resumen
        save_processing_summary(results)
        
        logger.info("\n=== FASE 4 COMPLETADA EXITOSAMENTE ===")
        
    except Exception as e:
        logger.error(f"Error fatal en Fase 4: {e}")
        raise

if __name__ == "__main__":
    main()

