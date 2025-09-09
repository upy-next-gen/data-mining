#!/usr/bin/env python3
"""
Fase 2: Validación de Archivos
Valida que los archivos encontrados tengan las columnas requeridas
"""

import json
import pandas as pd
import logging
from pathlib import Path
from datetime import datetime

# Configurar logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/fase2_validacion.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def validar_archivo_csv(ruta_archivo, columnas_requeridas, columnas_opcionales):
    """
    Valida que un archivo CSV tenga las columnas requeridas
    NOM_CD es opcional - si no existe, se puede procesar igual
    """
    try:
        # Leer solo las primeras 5 filas para validación rápida
        df = pd.read_csv(ruta_archivo, nrows=5)
        
        # Verificar columnas requeridas (sin NOM_CD)
        columnas_presentes = set(df.columns)
        columnas_faltantes = set(columnas_requeridas) - columnas_presentes
        
        if columnas_faltantes:
            logger.warning(f"Columnas requeridas faltantes en {ruta_archivo}: {columnas_faltantes}")
            return False, list(columnas_faltantes)
        
        # Verificar columnas opcionales (solo informativo)
        columnas_opcionales_faltantes = set(columnas_opcionales) - columnas_presentes
        if columnas_opcionales_faltantes:
            logger.info(f"Columnas opcionales faltantes (no afecta procesamiento): {columnas_opcionales_faltantes}")
        
        # Verificar BP1_1 si está presente
        if 'BP1_1' in df.columns and len(df) > 0:
            valores_unicos = df['BP1_1'].dropna().unique()
            valores_invalidos = [v for v in valores_unicos if v not in [1, 2, 9]]
            if valores_invalidos:
                logger.warning(f"Valores inválidos en BP1_1: {valores_invalidos}")
        
        return True, []
        
    except Exception as e:
        logger.error(f"Error al validar {ruta_archivo}: {str(e)}")
        return False, [str(e)]

def contar_registros_yucatan(ruta_archivo):
    """
    Cuenta cuántos registros son de Yucatán
    """
    try:
        # Leer solo columna NOM_ENT para optimización
        df = pd.read_csv(ruta_archivo, usecols=['NOM_ENT'], nrows=1000)
        count = (df['NOM_ENT'] == 'Yucatán').sum()
        logger.debug(f"Registros de Yucatán en {ruta_archivo}: {count}")
        return count
    except:
        return 0

def detectar_duplicados(archivos_validados):
    """
    Detecta archivos que podrían ser duplicados del mismo trimestre
    """
    duplicados = {}
    
    for archivo in archivos_validados:
        if archivo.get('trimestre_info'):
            clave = f"{archivo['trimestre_info']['año']}_T{archivo['trimestre_info']['trimestre']}"
            if clave not in duplicados:
                duplicados[clave] = []
            duplicados[clave].append(archivo)
    
    # Filtrar solo los que tienen múltiples archivos
    duplicados_reales = {k: v for k, v in duplicados.items() if len(v) > 1}
    
    if duplicados_reales:
        logger.warning(f"Duplicados detectados: {list(duplicados_reales.keys())}")
        
        # Seleccionar el mejor archivo por trimestre
        for trimestre, archivos in duplicados_reales.items():
            logger.info(f"Resolviendo duplicados para {trimestre}")
            
            # Criterio: archivo con más registros de Yucatán
            for archivo in archivos:
                archivo['yucatan_count'] = contar_registros_yucatan(archivo['ruta'])
            
            mejor_archivo = max(archivos, key=lambda x: x['yucatan_count'])
            logger.info(f"Archivo seleccionado: {mejor_archivo['nombre']} ({mejor_archivo['yucatan_count']} registros de Yucatán)")
            
            # Marcar los demás como duplicados
            for archivo in archivos:
                if archivo != mejor_archivo:
                    archivo['es_duplicado'] = True
                    archivo['duplicado_de'] = mejor_archivo['nombre']
    
    return archivos_validados

def main():
    """
    Función principal de la Fase 2
    """
    logger.info("=== FASE 2: VALIDACIÓN ===")
    
    # Cargar inventario de Fase 1
    inventario_path = 'temp/inventario_archivos_cb.json'
    if not Path(inventario_path).exists():
        logger.error(f"No se encontró el inventario en {inventario_path}")
        logger.error("Ejecute primero la Fase 1")
        return False
    
    with open(inventario_path, 'r', encoding='utf-8') as f:
        inventario = json.load(f)
    
    logger.info(f"Archivos a validar: {inventario['total_archivos']}")
    
    # Filtrar solo archivos de conjunto_de_datos principales
    archivos_principales = []
    for archivo in inventario['archivos']:
        # Solo procesar archivos que contienen "conjunto_de_datos" en la ruta
        # y que terminan con el patrón CB####.csv
        if 'conjunto_de_datos' in archivo['ruta'] and \
           'conjunto_de_datos_' in archivo['nombre'] and \
           archivo['nombre'].endswith('.csv'):
            archivos_principales.append(archivo)
    
    logger.info(f"Archivos principales de datos encontrados: {len(archivos_principales)}")
    
    # Validar cada archivo
    # NOM_CD ahora es opcional
    columnas_requeridas = ['NOM_ENT', 'NOM_MUN', 'BP1_1']
    columnas_opcionales = ['NOM_CD']
    archivos_validados = []
    archivos_invalidos = []
    
    for archivo in archivos_principales:
        logger.debug(f"Validando: {archivo['nombre']}")
        es_valido, errores = validar_archivo_csv(archivo['ruta'], columnas_requeridas, columnas_opcionales)
        
        if es_valido:
            archivo['validado'] = True
            archivo['fecha_validacion'] = datetime.now().isoformat()
            archivos_validados.append(archivo)
        else:
            archivo['validado'] = False
            archivo['errores'] = errores
            archivos_invalidos.append(archivo)
    
    # Detectar y resolver duplicados
    archivos_validados = detectar_duplicados(archivos_validados)
    
    # Filtrar archivos no duplicados
    archivos_finales = [a for a in archivos_validados if not a.get('es_duplicado', False)]
    
    logger.info(f"Archivos válidos: {len(archivos_validados)}")
    logger.info(f"Archivos inválidos: {len(archivos_invalidos)}")
    logger.info(f"Archivos finales (sin duplicados): {len(archivos_finales)}")
    
    # Guardar resultados (convertir numpy types a Python native types)
    import numpy as np
    
    def convert_numpy_types(obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, dict):
            return {k: convert_numpy_types(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [convert_numpy_types(item) for item in obj]
        return obj
    
    resultado_validacion = {
        'fecha_validacion': datetime.now().isoformat(),
        'total_analizados': len(archivos_principales),
        'total_validos': len(archivos_validados),
        'total_invalidos': len(archivos_invalidos),
        'total_finales': len(archivos_finales),
        'archivos_validos': convert_numpy_types(archivos_finales),
        'archivos_invalidos': convert_numpy_types(archivos_invalidos),
        'archivos_duplicados': convert_numpy_types([a for a in archivos_validados if a.get('es_duplicado', False)])
    }
    
    ruta_salida = 'temp/archivos_validados.json'
    with open(ruta_salida, 'w', encoding='utf-8') as f:
        json.dump(resultado_validacion, f, indent=2, ensure_ascii=False)
    
    logger.info(f"Resultados guardados en: {ruta_salida}")
    
    # Resumen
    logger.info("=== RESUMEN FASE 2 ===")
    logger.info(f"Total archivos principales: {len(archivos_principales)}")
    logger.info(f"Archivos válidos: {len(archivos_finales)}")
    logger.info(f"Archivos duplicados eliminados: {len(archivos_validados) - len(archivos_finales)}")
    logger.info(f"Archivos inválidos: {len(archivos_invalidos)}")
    
    if archivos_finales:
        logger.info("Trimestres disponibles:")
        for archivo in archivos_finales:
            if archivo.get('trimestre_info'):
                info = archivo['trimestre_info']
                logger.info(f"  - {info['año']} T{info['trimestre']}: {archivo['nombre']}")
    
    return len(archivos_finales) > 0

if __name__ == "__main__":
    exito = main()
    exit(0 if exito else 1)