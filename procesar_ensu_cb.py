#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Procesador de datos ENSU para análisis de percepción de seguridad.

Este script procesa el dataset ENSU (Encuesta Nacional de Seguridad Pública Urbana)
para extraer y agregar información sobre percepción de seguridad por entidad y municipio.

Author: Cascade AI
Date: 2025-09-09
"""
import os
import sys
import re
import logging
import argparse
import unicodedata
from typing import Dict, List, Tuple, Set, Optional, Union, Any
from datetime import datetime
import pandas as pd
import numpy as np


# Configuración de logging
def setup_logging(logs_dir: str, timestamped: bool = False) -> logging.Logger:
    """
    Configura el sistema de logging para el procesamiento.
    
    Args:
        logs_dir: Directorio donde guardar los logs
        timestamped: Si se debe incluir timestamp en el nombre del archivo
    
    Returns:
        Logger configurado
    """
    os.makedirs(logs_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = f"procesamiento_log_{timestamp}.log" if timestamped else "procesamiento_log.log"
    log_path = os.path.join(logs_dir, log_file)
    
    logger = logging.getLogger("procesador_ensu")
    logger.setLevel(logging.INFO)
    
    # Handler para archivo
    file_handler = logging.FileHandler(log_path, encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    
    # Handler para consola
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    # Formato de log
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    # Agregar handlers
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger


def normalizar_string(texto: str) -> str:
    """
    Normaliza un string: mayúsculas, sin acentos, Ñ→N, espacios normalizados.
    
    Args:
        texto: String a normalizar
    
    Returns:
        String normalizado
    """
    if pd.isna(texto) or not isinstance(texto, str):
        return ""
    
    # Convertir a mayúsculas
    texto = texto.upper()
    
    # Normalizar caracteres (eliminar acentos)
    texto = unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('ASCII')
    
    # Normalizar espacios
    texto = ' '.join(texto.split())
    
    return texto


def extraer_año_trimestre(nombre_archivo: str) -> Tuple[int, str]:
    """
    Extrae año y trimestre del nombre del archivo.
    
    Args:
        nombre_archivo: Nombre del archivo a analizar
    
    Returns:
        Tuple con (año, trimestre)
    """
    # Buscar patrón cb_MMAA o cb_MMYY en el nombre del archivo
    match = re.search(r'cb_(\d{2})(\d{2})', nombre_archivo.lower())
    
    if match:
        mes = int(match.group(1))
        año_corto = int(match.group(2))
        
        # Convertir año de 2 dígitos a 4 dígitos (asumiendo 2000+)
        año = 2000 + año_corto
        
        # Determinar trimestre según el mes
        if 1 <= mes <= 3:
            trimestre = "Q1"
        elif 4 <= mes <= 6:
            trimestre = "Q2"
        elif 7 <= mes <= 9:
            trimestre = "Q3"
        elif 10 <= mes <= 12:
            trimestre = "Q4"
        else:
            trimestre = "DESCONOCIDO"
            
        return año, trimestre
    else:
        return 0, "DESCONOCIDO"


def procesar_dataset(
    archivo_entrada: str, 
    directorio_salida: str,
    logger: logging.Logger,
    chunksize: Optional[int] = None
) -> Tuple[str, Dict[str, int]]:
    """
    Procesa el dataset ENSU y genera el archivo agregado.
    
    Args:
        archivo_entrada: Ruta al archivo CSV de entrada
        directorio_salida: Directorio donde guardar el archivo procesado
        logger: Logger para registro de eventos
        chunksize: Tamaño del chunk para procesamiento por lotes (None = todo de una vez)
    
    Returns:
        Tuple con (ruta_archivo_salida, estadísticas_procesamiento)
    """
    # Extraer año y trimestre del nombre del archivo
    nombre_base = os.path.basename(archivo_entrada)
    año, trimestre = extraer_año_trimestre(nombre_base)
    
    if año == 0 or trimestre == "DESCONOCIDO":
        # Si no se pudo extraer del nombre, asumimos 2025-Q2 según instrucciones
        año = 2025
        trimestre = "Q2"
        logger.warning(
            f"No se pudo determinar año/trimestre del nombre '{nombre_base}'. "
            f"Se asume {año}-{trimestre} por defecto."
        )
    else:
        logger.info(f"Se determinó periodo {año}-{trimestre} del nombre '{nombre_base}'")
    
    # Estadísticas de procesamiento
    stats = {
        "total_registros_entrada": 0,
        "registros_procesados": 0,
        "registros_excluidos_total": 0,
        "registros_excluidos_BP1_1_nulo": 0,
        "registros_excluidos_BP1_1_invalido": 0
    }
    
    # Columnas requeridas
    columnas_requeridas = ["NOM_ENT", "NOM_MUN", "BP1_1"]
    
    # Inicializar DataFrame para almacenar resultados agregados
    resultados_agregados = {}
    
    # Función de procesamiento para aplicar a cada chunk
    def procesar_chunk(chunk: pd.DataFrame) -> None:
        nonlocal stats, resultados_agregados
        
        # Verificar presencia de columnas requeridas
        if not all(col in chunk.columns for col in columnas_requeridas):
            columnas_faltantes = [col for col in columnas_requeridas if col not in chunk.columns]
            logger.error(f"Columnas requeridas faltantes: {columnas_faltantes}")
            sys.exit(1)
        
        # Actualizar contador total
        stats["total_registros_entrada"] += len(chunk)
        
        # Filtrar registros con BP1_1 válido (1, 2, 9)
        mascara_bp1_1_nulo = chunk["BP1_1"].isna()
        stats["registros_excluidos_BP1_1_nulo"] += mascara_bp1_1_nulo.sum()
        
        chunk_filtrado = chunk[~mascara_bp1_1_nulo].copy()
        
        # Convertir BP1_1 a numérico si no lo es
        if not pd.api.types.is_numeric_dtype(chunk_filtrado["BP1_1"]):
            chunk_filtrado["BP1_1"] = pd.to_numeric(chunk_filtrado["BP1_1"], errors="coerce")
        
        # Filtrar valores válidos de BP1_1
        mascara_bp1_1_valido = chunk_filtrado["BP1_1"].isin([1, 2, 9])
        stats["registros_excluidos_BP1_1_invalido"] += (~mascara_bp1_1_valido).sum()
        
        chunk_final = chunk_filtrado[mascara_bp1_1_valido].copy()
        stats["registros_procesados"] += len(chunk_final)
        
        # Normalizar nombres de entidad y municipio
        chunk_final["NOM_ENT"] = chunk_final["NOM_ENT"].apply(normalizar_string)
        chunk_final["NOM_MUN"] = chunk_final["NOM_MUN"].apply(normalizar_string)
        
        # Agregar conteos por entidad-municipio
        for (entidad, municipio), grupo in chunk_final.groupby(["NOM_ENT", "NOM_MUN"]):
            key = (entidad, municipio)
            
            if key not in resultados_agregados:
                resultados_agregados[key] = {
                    "TOTAL_REGISTROS": 0,
                    "TOTAL_SEGUROS": 0,
                    "TOTAL_INSEGUROS": 0,
                    "TOTAL_NO_RESPONDE": 0
                }
            
            # Actualizar conteos
            resultados_agregados[key]["TOTAL_REGISTROS"] += len(grupo)
            resultados_agregados[key]["TOTAL_SEGUROS"] += (grupo["BP1_1"] == 1).sum()
            resultados_agregados[key]["TOTAL_INSEGUROS"] += (grupo["BP1_1"] == 2).sum()
            resultados_agregados[key]["TOTAL_NO_RESPONDE"] += (grupo["BP1_1"] == 9).sum()
    
    # Procesar el archivo (en chunks o completo)
    logger.info(f"Iniciando procesamiento de {archivo_entrada}")
    
    if chunksize:
        logger.info(f"Procesando en chunks de {chunksize} registros")
        reader = pd.read_csv(archivo_entrada, chunksize=chunksize)
        for chunk in reader:
            procesar_chunk(chunk)
    else:
        logger.info("Procesando archivo completo en memoria")
        df = pd.read_csv(archivo_entrada)
        procesar_chunk(df)
    
    # Calcular porcentajes y crear DataFrame final
    filas = []
    
    for (entidad, municipio), conteos in resultados_agregados.items():
        total = conteos["TOTAL_REGISTROS"]
        
        # Calcular porcentajes con 2 decimales
        pct_seguros = round((conteos["TOTAL_SEGUROS"] / total) * 100, 2) if total > 0 else 0
        pct_inseguros = round((conteos["TOTAL_INSEGUROS"] / total) * 100, 2) if total > 0 else 0
        pct_no_responde = round((conteos["TOTAL_NO_RESPONDE"] / total) * 100, 2) if total > 0 else 0
        
        # Validar que los porcentajes sumen 100 (con margen de error por redondeo)
        suma_porcentajes = pct_seguros + pct_inseguros + pct_no_responde
        if abs(suma_porcentajes - 100) > 0.1:  # Permitir pequeño error de redondeo
            logger.warning(
                f"Los porcentajes para {entidad}-{municipio} no suman exactamente 100%: {suma_porcentajes}%"
            )
        
        filas.append({
            "NOM_ENT": entidad,
            "NOM_MUN": municipio,
            "TOTAL_REGISTROS": conteos["TOTAL_REGISTROS"],
            "TOTAL_SEGUROS": conteos["TOTAL_SEGUROS"],
            "TOTAL_INSEGUROS": conteos["TOTAL_INSEGUROS"],
            "TOTAL_NO_RESPONDE": conteos["TOTAL_NO_RESPONDE"],
            "PCT_SEGUROS": pct_seguros,
            "PCT_INSEGUROS": pct_inseguros,
            "PCT_NO_RESPONDE": pct_no_responde,
            "AÑO": año,
            "TRIMESTRE": trimestre
        })
    
    # Crear DataFrame final y guardar CSV
    df_resultados = pd.DataFrame(filas)
    
    # Ordenar columnas según el orden especificado
    columnas_orden = [
        "NOM_ENT", "NOM_MUN", "TOTAL_REGISTROS", "TOTAL_SEGUROS", "TOTAL_INSEGUROS", 
        "TOTAL_NO_RESPONDE", "PCT_SEGUROS", "PCT_INSEGUROS", "PCT_NO_RESPONDE", 
        "AÑO", "TRIMESTRE"
    ]
    df_resultados = df_resultados[columnas_orden]
    
    # Definir ruta de salida
    os.makedirs(directorio_salida, exist_ok=True)
    ruta_salida = os.path.join(
        directorio_salida, f"procesado_{año}_{trimestre}_cb.csv"
    )
    
    # Guardar resultado
    df_resultados.to_csv(ruta_salida, index=False)
    
    # Actualizar contadores de exclusión total
    stats["registros_excluidos_total"] = (
        stats["registros_excluidos_BP1_1_nulo"] + 
        stats["registros_excluidos_BP1_1_invalido"]
    )
    
    # Log de resultados
    logger.info(f"Procesamiento finalizado:")
    logger.info(f"  - Total registros de entrada: {stats['total_registros_entrada']}")
    logger.info(f"  - Registros procesados: {stats['registros_procesados']}")
    logger.info(f"  - Registros excluidos: {stats['registros_excluidos_total']}")
    logger.info(f"    * BP1_1 nulo: {stats['registros_excluidos_BP1_1_nulo']}")
    logger.info(f"    * BP1_1 inválido: {stats['registros_excluidos_BP1_1_invalido']}")
    logger.info(f"  - Archivo generado: {ruta_salida}")
    
    return ruta_salida, stats


def main():
    """Función principal del script."""
    # Parsear argumentos de línea de comandos
    parser = argparse.ArgumentParser(
        description="Procesador de datos ENSU para análisis de percepción de seguridad."
    )
    parser.add_argument(
        "--estado", 
        default="YUCATAN",
        help="Estado a filtrar (default: YUCATAN)"
    )
    parser.add_argument(
        "--salida", 
        default="data/yucatan-inseguridad/",
        help="Directorio de salida principal (default: data/yucatan-inseguridad/)"
    )
    parser.add_argument(
        "--reportes", 
        default="data/yucatan-inseguridad/reportes/",
        help="Directorio de reportes (default: data/yucatan-inseguridad/reportes/)"
    )
    parser.add_argument(
        "--logs", 
        default="data/yucatan-inseguridad/logs/",
        help="Directorio de logs (default: data/yucatan-inseguridad/logs/)"
    )
    parser.add_argument(
        "--chunksize", 
        type=int,
        help="Tamaño del chunk para procesamiento por lotes (opcional)"
    )
    parser.add_argument(
        "--timestamped", 
        action="store_true",
        help="Agregar timestamp a los nombres de archivo de salida"
    )
    
    args = parser.parse_args()
    
    # Configurar logging
    logger = setup_logging(args.logs, args.timestamped)
    
    # Validar archivo de entrada
    archivo_entrada = "conjunto_de_datos_ensu_cb_0625.csv"
    if not os.path.exists(archivo_entrada):
        logger.error(f"El archivo de entrada '{archivo_entrada}' no existe.")
        sys.exit(1)
    
    try:
        # Procesar dataset
        archivo_salida, estadisticas = procesar_dataset(
            archivo_entrada=archivo_entrada,
            directorio_salida=args.salida,
            logger=logger,
            chunksize=args.chunksize
        )
        
        logger.info(f"Procesamiento exitoso. Archivo generado: {archivo_salida}")
        
    except Exception as e:
        logger.exception(f"Error durante el procesamiento: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
