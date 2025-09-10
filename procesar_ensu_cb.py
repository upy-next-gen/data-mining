#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Procesador de archivos ENSU para cuestionario básico (CB).

Este script procesa los archivos CSV de ENSU identificados en el inventario,
genera agregados por municipio para el estado de Yucatán y guarda los 
resultados en archivos CSV estructurados por periodo.

Autor: Cascade AI
Fecha: 2025-09-10
"""

import os
import re
import sys
import json
import logging
import argparse
import unicodedata
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Union, Any, Set

import pandas as pd
import numpy as np
from tqdm import tqdm


def setup_logging(log_file: str) -> logging.Logger:
    """
    Configura el sistema de logging.
    
    Args:
        log_file: Ruta al archivo de log
    
    Returns:
        Logger configurado
    """
    # Crear directorio para logs si no existe
    log_dir = os.path.dirname(log_file)
    os.makedirs(log_dir, exist_ok=True)
    
    # Configurar logger
    logger = logging.getLogger("procesar_ensu_cb")
    logger.setLevel(logging.INFO)
    
    # Formato para los logs
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    
    # Handler para archivo
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.INFO)
    
    # Handler para consola
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)
    
    # Añadir handlers
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger


def cargar_inventario(ruta_inventario: str, logger: logging.Logger) -> List[Dict[str, Any]]:
    """
    Carga el inventario de archivos ENSU.
    
    Args:
        ruta_inventario: Ruta al archivo JSON de inventario
        logger: Logger para registrar eventos
        
    Returns:
        Lista de diccionarios con información de los archivos
    """
    try:
        with open(ruta_inventario, 'r', encoding='utf-8') as f:
            inventario = json.load(f)
        
        logger.info(f"Inventario cargado: {len(inventario)} archivos encontrados")
        return inventario
    except Exception as e:
        logger.error(f"Error al cargar inventario: {str(e)}")
        sys.exit(1)


def detectar_delimitador(archivo: str, logger: logging.Logger) -> str:
    """
    Detecta el delimitador de un archivo CSV.
    
    Args:
        archivo: Ruta al archivo CSV
        logger: Logger para registrar eventos
        
    Returns:
        Delimitador detectado (',' o ';')
    """
    try:
        with open(archivo, 'r', encoding='utf-8', errors='replace') as f:
            primera_linea = f.readline().strip()
            segunda_linea = f.readline().strip() if primera_linea else ""
        
        # Contar delimitadores en las dos primeras líneas
        comas_1 = primera_linea.count(',')
        puntoycomas_1 = primera_linea.count(';')
        
        comas_2 = segunda_linea.count(',')
        puntoycomas_2 = segunda_linea.count(';')
        
        # Usar la línea con más delimitadores
        comas = max(comas_1, comas_2)
        puntoycomas = max(puntoycomas_1, puntoycomas_2)
        
        if puntoycomas > comas:
            logger.debug(f"Delimitador detectado para {archivo}: ';'")
            return ';'
        
        logger.debug(f"Delimitador detectado para {archivo}: ','")
        return ','
    except Exception as e:
        logger.warning(f"Error detectando delimitador para {archivo}: {str(e)}. Usando ',' por defecto.")
        return ','


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
    
    # Normalizar espacios (eliminar múltiples espacios)
    texto = ' '.join(texto.split())
    
    return texto


def normalizar_columnas(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza los nombres de las columnas del DataFrame.
    
    Args:
        df: DataFrame a normalizar
        
    Returns:
        DataFrame con columnas normalizadas
    """
    df.columns = [normalizar_string(col) for col in df.columns]
    return df


def verificar_columnas_requeridas(df: pd.DataFrame, columnas_requeridas: List[str], logger: logging.Logger) -> bool:
    """
    Verifica que el DataFrame contenga las columnas requeridas.
    
    Args:
        df: DataFrame a verificar
        columnas_requeridas: Lista de columnas requeridas
        logger: Logger para registrar eventos
        
    Returns:
        True si contiene todas las columnas, False en caso contrario
    """
    columnas_faltantes = [col for col in columnas_requeridas if col not in df.columns]
    
    if columnas_faltantes:
        logger.error(f"Columnas requeridas faltantes: {columnas_faltantes}")
        return False
    
    return True


def procesar_archivo(
    item: Dict[str, Any],
    estado: str,
    directorio_salida: str,
    chunksize: Optional[int],
    logger: logging.Logger
) -> Dict[str, Any]:
    """
    Procesa un archivo ENSU y genera el agregado por municipio.
    
    Args:
        item: Elemento del inventario con información del archivo
        estado: Estado a filtrar (normalizado)
        directorio_salida: Directorio donde guardar el resultado
        chunksize: Tamaño del chunk para procesamiento (opcional)
        logger: Logger para registrar eventos
        
    Returns:
        Diccionario con estadísticas del procesamiento
    """
    archivo_entrada = item["csv_raw"]
    anio = item["anio"]
    trimestre = item["trimestre"]
    periodo = f"{anio}-{trimestre}"
    
    logger.info(f"Procesando archivo {archivo_entrada} para periodo {periodo}")
    
    # Determinar nombre de archivo de salida
    procesados_dir = os.path.join(directorio_salida, "procesados")
    os.makedirs(procesados_dir, exist_ok=True)
    
    archivo_salida = os.path.join(procesados_dir, f"procesado_{anio}_{trimestre}_cb.csv")
    
    # Verificar si ya existe un archivo procesado
    if os.path.exists(archivo_salida):
        logger.warning(f"Ya existe un archivo procesado para {periodo}. Verificando si ha cambiado...")
        
        # Comparar tamaño
        if "filas" in item:
            try:
                df_existente = pd.read_csv(archivo_salida)
                if len(df_existente) > 0:
                    logger.info(f"Manteniendo archivo existente para {periodo} ({len(df_existente)} registros)")
                    return {
                        "periodo": periodo,
                        "anio": anio,
                        "trimestre": trimestre,
                        "archivo_entrada": archivo_entrada,
                        "archivo_salida": archivo_salida,
                        "estado": "mantenido",
                        "municipios": len(df_existente["NOM_MUN"].unique()),
                        "registros_totales": len(df_existente),
                        "registros_filtrados": 0
                    }
            except Exception as e:
                logger.warning(f"Error leyendo archivo existente: {str(e)}. Se sobrescribirá.")
    
    # Inicializar estadísticas
    stats = {
        "registros_totales": 0,
        "registros_yucatan": 0,
        "registros_filtrados": 0,
        "registros_bp1_1_nulos": 0,
        "registros_bp1_1_invalidos": 0
    }
    
    # Columnas requeridas
    columnas_requeridas = ["NOM_ENT", "NOM_MUN", "BP1_1"]
    
    # Diccionario para almacenar agregados por municipio
    agregados = {}
    
    try:
        # Detectar delimitador
        delimitador = detectar_delimitador(archivo_entrada, logger)
        
        # Función para procesar un chunk o el DataFrame completo
        def procesar_chunk(df: pd.DataFrame) -> None:
            nonlocal agregados, stats
            
            # Normalizar columnas
            df = normalizar_columnas(df)
            
            # Verificar columnas requeridas
            if not verificar_columnas_requeridas(df, columnas_requeridas, logger):
                logger.error(f"Archivo {archivo_entrada} no contiene todas las columnas requeridas")
                return
            
            # Actualizar contador total
            stats["registros_totales"] += len(df)
            
            # Filtrar registros con BP1_1 nulos
            registros_bp1_1_nulos = df["BP1_1"].isna().sum()
            stats["registros_bp1_1_nulos"] += registros_bp1_1_nulos
            
            df = df.dropna(subset=["BP1_1"])
            
            # Convertir BP1_1 a numérico si es necesario
            if not pd.api.types.is_numeric_dtype(df["BP1_1"]):
                df["BP1_1"] = pd.to_numeric(df["BP1_1"], errors="coerce")
            
            # Filtrar valores válidos de BP1_1 (1, 2, 9)
            validos_bp1_1 = df["BP1_1"].isin([1, 2, 9])
            registros_bp1_1_invalidos = (~validos_bp1_1).sum()
            stats["registros_bp1_1_invalidos"] += registros_bp1_1_invalidos
            
            df = df[validos_bp1_1]
            
            # Normalizar nombres de entidad y municipio
            df["NOM_ENT"] = df["NOM_ENT"].apply(normalizar_string)
            df["NOM_MUN"] = df["NOM_MUN"].apply(normalizar_string)
            
            # Filtrar por estado
            df_estado = df[df["NOM_ENT"] == estado]
            stats["registros_yucatan"] += len(df_estado)
            
            # Agregar por municipio
            for (ent, mun), grupo in df_estado.groupby(["NOM_ENT", "NOM_MUN"]):
                key = (ent, mun)
                
                if key not in agregados:
                    agregados[key] = {
                        "TOTAL_REGISTROS": 0,
                        "TOTAL_SEGUROS": 0,
                        "TOTAL_INSEGUROS": 0,
                        "TOTAL_NO_RESPONDE": 0
                    }
                
                # Actualizar contadores
                agregados[key]["TOTAL_REGISTROS"] += len(grupo)
                agregados[key]["TOTAL_SEGUROS"] += (grupo["BP1_1"] == 1).sum()
                agregados[key]["TOTAL_INSEGUROS"] += (grupo["BP1_1"] == 2).sum()
                agregados[key]["TOTAL_NO_RESPONDE"] += (grupo["BP1_1"] == 9).sum()
        
        # Procesar el archivo (en chunks o completo)
        if chunksize:
            logger.info(f"Procesando {archivo_entrada} en chunks de {chunksize}")
            chunks = pd.read_csv(archivo_entrada, chunksize=chunksize, encoding='utf-8', 
                               delimiter=delimitador, on_bad_lines='skip', low_memory=False)
            
            for chunk in tqdm(chunks, desc=f"Procesando {os.path.basename(archivo_entrada)}"):
                procesar_chunk(chunk)
        else:
            logger.info(f"Procesando {archivo_entrada} completo")
            df = pd.read_csv(archivo_entrada, encoding='utf-8', delimiter=delimitador,
                           on_bad_lines='skip', low_memory=False)
            procesar_chunk(df)
        
        # Calcular filtrados totales
        stats["registros_filtrados"] = stats["registros_bp1_1_nulos"] + stats["registros_bp1_1_invalidos"]
        
        # Construir DataFrame final
        filas = []
        for (ent, mun), conteos in agregados.items():
            total = conteos["TOTAL_REGISTROS"]
            
            # Calcular porcentajes
            pct_seguros = round((conteos["TOTAL_SEGUROS"] / total) * 100, 2) if total > 0 else 0
            pct_inseguros = round((conteos["TOTAL_INSEGUROS"] / total) * 100, 2) if total > 0 else 0
            pct_no_responde = round((conteos["TOTAL_NO_RESPONDE"] / total) * 100, 2) if total > 0 else 0
            
            # Ajustar para que sumen exactamente 100%
            ajuste = 100 - (pct_seguros + pct_inseguros + pct_no_responde)
            if abs(ajuste) < 0.1:  # Ajustar solo si la diferencia es pequeña
                pct_inseguros = round(pct_inseguros + ajuste, 2)
            
            filas.append({
                "NOM_ENT": ent,
                "NOM_MUN": mun,
                "TOTAL_REGISTROS": conteos["TOTAL_REGISTROS"],
                "TOTAL_SEGUROS": conteos["TOTAL_SEGUROS"],
                "TOTAL_INSEGUROS": conteos["TOTAL_INSEGUROS"],
                "TOTAL_NO_RESPONDE": conteos["TOTAL_NO_RESPONDE"],
                "PCT_SEGUROS": pct_seguros,
                "PCT_INSEGUROS": pct_inseguros,
                "PCT_NO_RESPONDE": pct_no_responde,
                "AÑO": anio,
                "TRIMESTRE": trimestre,
                "PERIODO": periodo
            })
        
        # Si no hay datos para el estado, reportar y salir
        if not filas:
            logger.warning(f"No se encontraron datos para {estado} en {archivo_entrada}")
            return {
                "periodo": periodo,
                "anio": anio,
                "trimestre": trimestre,
                "archivo_entrada": archivo_entrada,
                "archivo_salida": None,
                "estado": "sin_datos",
                "municipios": 0,
                "registros_totales": stats["registros_totales"],
                "registros_filtrados": stats["registros_filtrados"]
            }
        
        # Crear DataFrame final
        df_final = pd.DataFrame(filas)
        
        # Ordenar columnas
        columnas_orden = [
            "NOM_ENT", "NOM_MUN", "TOTAL_REGISTROS", "TOTAL_SEGUROS", "TOTAL_INSEGUROS", 
            "TOTAL_NO_RESPONDE", "PCT_SEGUROS", "PCT_INSEGUROS", "PCT_NO_RESPONDE", 
            "AÑO", "TRIMESTRE", "PERIODO"
        ]
        
        df_final = df_final[columnas_orden]
        
        # Guardar resultado
        df_final.to_csv(archivo_salida, index=False)
        
        logger.info(f"Archivo procesado guardado en {archivo_salida}")
        
        # Retornar estadísticas
        return {
            "periodo": periodo,
            "anio": anio,
            "trimestre": trimestre,
            "archivo_entrada": archivo_entrada,
            "archivo_salida": archivo_salida,
            "estado": "procesado",
            "municipios": len(filas),
            "registros_totales": stats["registros_totales"],
            "registros_yucatan": stats["registros_yucatan"],
            "registros_filtrados": stats["registros_filtrados"]
        }
        
    except Exception as e:
        logger.exception(f"Error procesando {archivo_entrada}: {str(e)}")
        return {
            "periodo": periodo,
            "anio": anio,
            "trimestre": trimestre,
            "archivo_entrada": archivo_entrada,
            "archivo_salida": None,
            "estado": "error",
            "error": str(e),
            "municipios": 0,
            "registros_totales": stats["registros_totales"],
            "registros_filtrados": stats["registros_filtrados"]
        }


def generar_resumen(resultados: List[Dict[str, Any]], output_dir: str, logger: logging.Logger) -> str:
    """
    Genera un archivo JSON con el resumen del procesamiento.
    
    Args:
        resultados: Lista de resultados del procesamiento
        output_dir: Directorio base de salida
        logger: Logger para registrar eventos
        
    Returns:
        Ruta al archivo de resumen
    """
    # Crear directorio para logs si no existe
    logs_dir = os.path.join(output_dir, "logs")
    os.makedirs(logs_dir, exist_ok=True)
    
    # Clase para manejar tipos NumPy en JSON
    class NumpyEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, np.integer):
                return int(obj)
            if isinstance(obj, np.floating):
                return float(obj)
            if isinstance(obj, np.ndarray):
                return obj.tolist()
            return super(NumpyEncoder, self).default(obj)
    
    # Estadísticas por año
    stats_por_anio = {}
    
    # Estadísticas globales
    stats_globales = {
        "total_archivos": len(resultados),
        "archivos_procesados": sum(1 for r in resultados if r["estado"] == "procesado"),
        "archivos_mantenidos": sum(1 for r in resultados if r["estado"] == "mantenido"),
        "archivos_sin_datos": sum(1 for r in resultados if r["estado"] == "sin_datos"),
        "archivos_con_error": sum(1 for r in resultados if r["estado"] == "error"),
        "total_registros": int(sum(r["registros_totales"] for r in resultados)),
        "total_registros_yucatan": int(sum(r.get("registros_yucatan", 0) for r in resultados)),
        "total_registros_filtrados": int(sum(r["registros_filtrados"] for r in resultados)),
        "total_municipios": 0,  # Se calculará después
        "periodos": sorted(set(r["periodo"] for r in resultados))
    }
    
    # Calcular estadísticas por año
    for resultado in resultados:
        anio = int(resultado["anio"])  # Convertir a int nativo de Python
        
        if anio not in stats_por_anio:
            stats_por_anio[anio] = {
                "archivos": 0,
                "registros": 0,
                "registros_yucatan": 0,
                "trimestres": set()
            }
        
        stats_por_anio[anio]["archivos"] += 1
        stats_por_anio[anio]["registros"] += int(resultado["registros_totales"])
        stats_por_anio[anio]["registros_yucatan"] += int(resultado.get("registros_yucatan", 0))
        stats_por_anio[anio]["trimestres"].add(resultado["trimestre"])
    
    # Convertir sets a listas para JSON
    for anio in stats_por_anio:
        stats_por_anio[anio]["trimestres"] = sorted(list(stats_por_anio[anio]["trimestres"]))
    
    # Calcular municipios únicos
    municipios_unicos = set()
    for resultado in resultados:
        if resultado["estado"] in ["procesado", "mantenido"] and resultado["archivo_salida"]:
            try:
                df = pd.read_csv(resultado["archivo_salida"])
                municipios_unicos.update(df["NOM_MUN"].unique())
            except Exception as e:
                logger.warning(f"Error leyendo {resultado['archivo_salida']}: {str(e)}")
    
    stats_globales["total_municipios"] = len(municipios_unicos)
    
    # Crear estructura completa del resumen
    resumen = {
        "fecha_procesamiento": datetime.now().isoformat(),
        "estadisticas_globales": stats_globales,
        "estadisticas_por_anio": stats_por_anio,
        "resultados_detallados": resultados
    }
    
    # Guardar resumen
    resumen_path = os.path.join(logs_dir, "resumen_procesamiento.json")
    with open(resumen_path, 'w', encoding='utf-8') as f:
        json.dump(resumen, f, indent=2, ensure_ascii=False, cls=NumpyEncoder)
    
    logger.info(f"Resumen guardado en {resumen_path}")
    return resumen_path


def main():
    """Función principal del script."""
    parser = argparse.ArgumentParser(
        description="Procesador de archivos ENSU para cuestionario básico (CB)"
    )
    parser.add_argument(
        "--inventario",
        default=".\\data\\yucatan-inseguridad\\logs\\inventario_ensu.json",
        help="Ruta al archivo de inventario"
    )
    parser.add_argument(
        "--out",
        default=".\\data\\yucatan-inseguridad",
        help="Directorio base de salida"
    )
    parser.add_argument(
        "--estado",
        default="YUCATAN",
        help="Estado a filtrar"
    )
    parser.add_argument(
        "--chunksize",
        type=int,
        help="Tamaño del chunk para procesamiento"
    )
    parser.add_argument(
        "--log",
        default=".\\data\\yucatan-inseguridad\\logs\\procesador.log",
        help="Ruta al archivo de log"
    )
    
    args = parser.parse_args()
    
    # Normalizar estado
    estado = normalizar_string(args.estado)
    
    # Configurar logging
    logger = setup_logging(args.log)
    
    try:
        logger.info("Iniciando procesador ENSU CB")
        logger.info(f"Estado a filtrar: {estado}")
        
        # Verificar que exista el archivo de inventario
        if not os.path.exists(args.inventario):
            logger.error(f"El archivo de inventario '{args.inventario}' no existe")
            sys.exit(1)
        
        # Cargar inventario
        inventario = cargar_inventario(args.inventario, logger)
        
        # Procesar cada archivo
        resultados = []
        
        for item in tqdm(inventario, desc="Procesando archivos"):
            resultado = procesar_archivo(
                item=item,
                estado=estado,
                directorio_salida=args.out,
                chunksize=args.chunksize,
                logger=logger
            )
            resultados.append(resultado)
        
        # Generar resumen
        resumen_path = generar_resumen(resultados, args.out, logger)
        
        # Imprimir resumen
        print("\n--- RESUMEN DE PROCESAMIENTO ---")
        print(f"Total archivos procesados: {sum(1 for r in resultados if r['estado'] == 'procesado')}")
        print(f"Archivos mantenidos: {sum(1 for r in resultados if r['estado'] == 'mantenido')}")
        print(f"Archivos sin datos: {sum(1 for r in resultados if r['estado'] == 'sin_datos')}")
        print(f"Archivos con error: {sum(1 for r in resultados if r['estado'] == 'error')}")
        print(f"Resumen guardado en: {resumen_path}")
        
        logger.info("Procesador finalizado exitosamente")
        
    except Exception as e:
        logger.exception(f"Error en el procesador: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
