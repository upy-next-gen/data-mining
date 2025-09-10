#!/usr/bin/env python3
"""
Fase 4: Procesamiento Principal
Procesa cada archivo pendiente y genera los datasets de Yucatán
"""

import json
import pandas as pd
import logging
from pathlib import Path
from datetime import datetime
import os

# Configurar logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/fase4_procesamiento.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def procesar_archivo_ensu(archivo_info):
    """
    Procesa un archivo ENSU individual
    """
    ruta = archivo_info['ruta']
    nombre = archivo_info['nombre']
    
    logger.info(f"Procesando archivo: {nombre}")
    logger.debug(f"Ruta completa: {ruta}")
    
    try:
        # Leer archivo completo
        logger.debug("Leyendo archivo CSV...")
        df = pd.read_csv(ruta, low_memory=False)
        logger.info(f"  - Filas totales: {len(df)}")
        
        # Validar columnas requeridas (NOM_CD es opcional)
        columnas_requeridas = ['NOM_ENT', 'NOM_MUN', 'BP1_1']
        columnas_presentes = set(df.columns)
        columnas_faltantes = set(columnas_requeridas) - columnas_presentes
        
        if columnas_faltantes:
            logger.error(f"  - ERROR: Columnas requeridas faltantes: {columnas_faltantes}")
            return None
        
        # Agregar NOM_CD si no existe
        if 'NOM_CD' not in df.columns:
            logger.info(f"  - NOM_CD no encontrada, usando 'SIN_CIUDAD' como valor por defecto")
            df['NOM_CD'] = 'SIN_CIUDAD'
        
        # Filtrar solo Yucatán (diferentes formatos según el año)
        logger.debug("Filtrando datos de Yucatán...")
        # Limpiar espacios y caracteres de retorno, luego comparar
        df['NOM_ENT_CLEAN'] = df['NOM_ENT'].str.strip().str.upper()
        df_yucatan = df[df['NOM_ENT_CLEAN'].str.contains('YUCAT', case=False, na=False)].copy()
        registros_yucatan = len(df_yucatan)
        logger.info(f"  - Registros de Yucatán: {registros_yucatan}")
        
        if registros_yucatan == 0:
            logger.warning(f"  - ADVERTENCIA: No hay registros de Yucatán")
            # Política: procesar aunque sean mínimos
            return {
                'archivo_original': nombre,
                'registros_totales': len(df),
                'registros_yucatan': 0,
                'dataframe': pd.DataFrame(),
                'advertencia': 'Sin registros de Yucatán'
            }
        
        # Validar valores de BP1_1
        valores_bp1_1 = df_yucatan['BP1_1'].dropna().unique()
        valores_invalidos = [v for v in valores_bp1_1 if v not in [1, 2, 9]]
        if valores_invalidos:
            logger.warning(f"  - Valores inválidos en BP1_1: {valores_invalidos}")
        
        # Calcular estadísticas por municipio y ciudad
        logger.debug("Calculando estadísticas agregadas...")
        estadisticas = []
        
        for (nom_ent, nom_mun, nom_cd), grupo in df_yucatan.groupby(['NOM_ENT', 'NOM_MUN', 'NOM_CD']):
            total_seguros = (grupo['BP1_1'] == 1).sum()
            total_inseguros = (grupo['BP1_1'] == 2).sum()
            total_no_responde = (grupo['BP1_1'] == 9).sum()
            total_registros = len(grupo)
            
            # Calcular porcentajes
            pct_seguros = (total_seguros / total_registros * 100) if total_registros > 0 else 0
            pct_inseguros = (total_inseguros / total_registros * 100) if total_registros > 0 else 0
            pct_no_responde = (total_no_responde / total_registros * 100) if total_registros > 0 else 0
            
            estadisticas.append({
                'NOM_ENT': nom_ent,
                'NOM_MUN': nom_mun,
                'NOM_CD': nom_cd,
                'TOTAL_REGISTROS': total_registros,
                'TOTAL_SEGUROS': total_seguros,
                'TOTAL_INSEGUROS': total_inseguros,
                'TOTAL_NO_RESPONDE': total_no_responde,
                'PCT_SEGUROS': round(pct_seguros, 2),
                'PCT_INSEGUROS': round(pct_inseguros, 2),
                'PCT_NO_RESPONDE': round(pct_no_responde, 2)
            })
        
        df_estadisticas = pd.DataFrame(estadisticas)
        logger.info(f"  - Municipios/ciudades procesados: {len(df_estadisticas)}")
        
        # Agregar información del trimestre
        if archivo_info.get('trimestre_info'):
            info = archivo_info['trimestre_info']
            df_estadisticas['AÑO'] = info['año']
            df_estadisticas['TRIMESTRE'] = info['trimestre']
        
        return {
            'archivo_original': nombre,
            'registros_totales': len(df),
            'registros_yucatan': registros_yucatan,
            'municipios_procesados': len(df_estadisticas),
            'dataframe': df_estadisticas,
            'trimestre_info': archivo_info.get('trimestre_info')
        }
        
    except Exception as e:
        logger.error(f"  - ERROR al procesar: {str(e)}")
        return None

def guardar_resultado(resultado, directorio_salida='data/yucatan_processed'):
    """
    Guarda el resultado procesado en un archivo CSV
    """
    if resultado is None or resultado['dataframe'].empty:
        logger.warning("No hay datos para guardar")
        return None
    
    # Crear directorio si no existe
    os.makedirs(directorio_salida, exist_ok=True)
    
    # Generar nombre del archivo
    info = resultado.get('trimestre_info')
    if info:
        nombre_salida = f"yucatan_{info['año']}_T{info['trimestre']}_CB.csv"
    else:
        # Usar timestamp si no hay info de trimestre
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        nombre_salida = f"yucatan_procesado_{timestamp}.csv"
    
    ruta_salida = os.path.join(directorio_salida, nombre_salida)
    
    # Guardar CSV
    resultado['dataframe'].to_csv(ruta_salida, index=False)
    logger.info(f"  - Archivo guardado: {ruta_salida}")
    
    return ruta_salida

def main():
    """
    Función principal de la Fase 4
    """
    logger.info("=== FASE 4: PROCESAMIENTO PRINCIPAL ===")
    
    # Cargar archivos pendientes de Fase 3
    pendientes_path = 'temp/archivos_pendientes.json'
    if not Path(pendientes_path).exists():
        logger.error(f"No se encontró el archivo de pendientes en {pendientes_path}")
        logger.error("Ejecute primero la Fase 3")
        return False
    
    with open(pendientes_path, 'r', encoding='utf-8') as f:
        datos_pendientes = json.load(f)
    
    archivos_pendientes = datos_pendientes['archivos_pendientes']
    logger.info(f"Archivos a procesar: {len(archivos_pendientes)}")
    
    # Procesar cada archivo
    resultados_procesamiento = []
    archivos_exitosos = 0
    archivos_con_error = 0
    archivos_sin_datos = 0
    
    for i, archivo in enumerate(archivos_pendientes, 1):
        logger.info(f"\n[{i}/{len(archivos_pendientes)}] Procesando: {archivo['nombre']}")
        
        resultado = procesar_archivo_ensu(archivo)
        
        if resultado:
            if resultado['registros_yucatan'] > 0:
                # Guardar resultado
                ruta_guardada = guardar_resultado(resultado)
                if ruta_guardada:
                    archivos_exitosos += 1
                    resultado['archivo_guardado'] = ruta_guardada
                    resultado['fecha_procesamiento'] = datetime.now().isoformat()
                    resultados_procesamiento.append(resultado)
            else:
                archivos_sin_datos += 1
                logger.warning(f"  - Sin datos de Yucatán, pero continuando...")
                resultado['archivo_guardado'] = None
                resultado['fecha_procesamiento'] = datetime.now().isoformat()
                resultados_procesamiento.append(resultado)
        else:
            archivos_con_error += 1
            logger.error(f"  - Error al procesar archivo")
    
    # Guardar resumen de procesamiento
    resumen = {
        'fecha_procesamiento': datetime.now().isoformat(),
        'total_archivos': len(archivos_pendientes),
        'archivos_exitosos': archivos_exitosos,
        'archivos_con_error': archivos_con_error,
        'archivos_sin_datos': archivos_sin_datos,
        'resultados': [
            {
                'archivo': r['archivo_original'],
                'registros_totales': r['registros_totales'],
                'registros_yucatan': r['registros_yucatan'],
                'municipios': r.get('municipios_procesados', 0),
                'archivo_guardado': r.get('archivo_guardado'),
                'trimestre': r.get('trimestre_info')
            }
            for r in resultados_procesamiento
        ]
    }
    
    resumen_path = 'temp/resumen_procesamiento.json'
    with open(resumen_path, 'w', encoding='utf-8') as f:
        json.dump(resumen, f, indent=2, ensure_ascii=False)
    
    logger.info(f"\nResumen guardado en: {resumen_path}")
    
    # Resumen final
    logger.info("\n=== RESUMEN FASE 4 ===")
    logger.info(f"Total archivos procesados: {len(archivos_pendientes)}")
    logger.info(f"Archivos exitosos: {archivos_exitosos}")
    logger.info(f"Archivos sin datos de Yucatán: {archivos_sin_datos}")
    logger.info(f"Archivos con error: {archivos_con_error}")
    
    # Mostrar estadísticas generales
    if resultados_procesamiento:
        total_registros = sum(r['registros_yucatan'] for r in resultados_procesamiento)
        logger.info(f"Total registros de Yucatán procesados: {total_registros}")
    
    return archivos_exitosos > 0

if __name__ == "__main__":
    exito = main()
    exit(0 if exito else 1)