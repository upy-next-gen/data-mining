#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Orquestador de insumos ENSU del INEGI.

Este script descubre y cataloga paquetes ENSU, copiando los archivos relevantes
a una estructura estandarizada y generando un inventario JSON.

Autor: Raúl C.
Fecha: 2025-09-10
"""

import os
import re
import sys
import json
import shutil
import logging
import argparse
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Union, Any

import pandas as pd
import chardet
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
    logger = logging.getLogger("orquestador_ensu")
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


def detectar_encoding(archivo: str) -> str:
    """
    Detecta el encoding de un archivo.
    
    Args:
        archivo: Ruta al archivo
    
    Returns:
        Encoding detectado (o utf-8 por defecto)
    """
    try:
        with open(archivo, 'rb') as f:
            resultado = chardet.detect(f.read(10000))
        
        # Usar el encoding detectado, o utf-8 si la confianza es baja
        if resultado['confidence'] > 0.7:
            return resultado['encoding']
        return 'utf-8'
    except Exception:
        return 'utf-8'


def detectar_delimitador(archivo: str, encoding: str) -> str:
    """
    Detecta el delimitador de un archivo CSV.
    
    Args:
        archivo: Ruta al archivo CSV
        encoding: Encoding del archivo
        
    Returns:
        Delimitador detectado (',' o ';')
    """
    try:
        with open(archivo, 'r', encoding=encoding, errors='replace') as f:
            primera_linea = f.readline().strip()
        
        # Contar delimitadores
        comas = primera_linea.count(',')
        puntoycomas = primera_linea.count(';')
        
        # Decidir delimitador
        if puntoycomas > comas:
            return ';'
        return ','
    except Exception:
        return ','


def extraer_anio_trimestre(ruta: str) -> Tuple[int, str]:
    """
    Extrae año y trimestre de la ruta o nombre del archivo/directorio.
    
    Args:
        ruta: Ruta al archivo o directorio
        
    Returns:
        Tupla con (anio, trimestre) o (0, "")
    """
    # Normalizar path separators y obtener el nombre base
    ruta = str(ruta).replace('\\', '/').lower()
    nombre = os.path.basename(ruta)
    ruta_completa = ruta
    
    # Patrones a buscar
    patrones = [
        # Patrón 1: YYYY_Qt (ej. 2019_2t)
        (r'(20\d{2})[^0-9]([1-4])t', lambda m: (int(m.group(1)), f"Q{m.group(2)}")),
        
        # Patrón 2: MM_YYYY (ej. 03_2019)
        (r'(\d{1,2})_(20\d{2})', lambda m: (int(m.group(2)), _mes_a_trimestre(int(m.group(1))))),
        
        # Patrón 3: YYYY_MM (ej. 2019_03)
        (r'(20\d{2})_(\d{1,2})', lambda m: (int(m.group(1)), _mes_a_trimestre(int(m.group(2))))),
        
        # Patrón 4: YYYY_QN (ej. 2019_Q2)
        (r'(20\d{2})_(q[1-4])', lambda m: (int(m.group(1)), m.group(2).upper())),
        
        # Patrón 5: cb_MMYY (ej. cb_0625 = junio 2025)
        (r'cb[_-]?(\d{2})(\d{2})', lambda m: (2000 + int(m.group(2)), _mes_a_trimestre(int(m.group(1))))),
    ]
    
    # Intentar cada patrón en nombre y ruta completa
    for texto in [nombre, ruta_completa]:
        for patron, extractor in patrones:
            match = re.search(patron, texto)
            if match:
                try:
                    return extractor(match)
                except Exception:
                    continue
    
    return (0, "")


def _mes_a_trimestre(mes: int) -> str:
    """
    Convierte un mes a su trimestre correspondiente.
    
    Args:
        mes: Número de mes (1-12)
        
    Returns:
        Trimestre (Q1-Q4)
    """
    if 1 <= mes <= 3:
        return "Q1"
    elif 4 <= mes <= 6:
        return "Q2"
    elif 7 <= mes <= 9:
        return "Q3"
    elif 10 <= mes <= 12:
        return "Q4"
    else:
        return ""


def buscar_candidatos_ensu(
    root_dir: str, 
    logger: logging.Logger
) -> List[Dict[str, Any]]:
    """
    Busca recursivamente candidatos de archivos ENSU CB.
    
    Args:
        root_dir: Directorio raíz donde buscar
        logger: Logger para registrar eventos
        
    Returns:
        Lista de diccionarios con información de candidatos encontrados
    """
    candidatos = []
    root_path = Path(root_dir)
    
    logger.info(f"Buscando candidatos ENSU en {root_dir}...")
    
    # Patrones específicos a buscar para archivos CB de ENSU
    # Esto busca archivos CSV de datos (no diccionarios) en las rutas esperadas
    patrones_busqueda = [
        # Patrón para conjunto de datos (no diccionario)
        "**/conjunto_de_datos_cb*/**/conjunto_de_datos/*.csv",
        # Patrón alternativo 
        "**/conjunto_de_datos_CB*/**/conjunto_de_datos/*.csv",
        # Otros patrones posibles
        "**/*cb_ensu*/**/conjunto_de_datos/*.csv",
        "**/*CB_ENSU*/**/conjunto_de_datos/*.csv"
    ]
    
    # Unificar todos los archivos encontrados
    archivos_encontrados = []
    for patron in patrones_busqueda:
        archivos_patron = list(root_path.glob(patron))
        logger.info(f"Patrón '{patron}': {len(archivos_patron)} archivos encontrados")
        archivos_encontrados.extend(archivos_patron)
    
    # Eliminar duplicados (por si algún archivo coincide con múltiples patrones)
    archivos_unicos = list(set(archivos_encontrados))
    logger.info(f"Total de archivos únicos encontrados: {len(archivos_unicos)}")
    
    # Procesar cada archivo encontrado
    for archivo in tqdm(archivos_unicos, desc="Procesando candidatos"):
        try:
            nombre_lower = str(archivo).lower()
            
            # Verificar si es un archivo de datos (no un diccionario)
            if 'diccionario' in nombre_lower:
                logger.debug(f"Omitiendo diccionario: {archivo}")
                continue
                
            # Extraer año y trimestre
            anio, trimestre = extraer_anio_trimestre(archivo)
            
            # Si no se pudo extraer, intentar desde el directorio padre
            if anio == 0:
                anio, trimestre = extraer_anio_trimestre(archivo.parent)
            
            # Si todavía no se pudo, subir un nivel más
            if anio == 0 and archivo.parent and archivo.parent.parent:
                anio, trimestre = extraer_anio_trimestre(archivo.parent.parent)
            
            # Si todavía no se pudo, subir otro nivel más
            if anio == 0 and archivo.parent and archivo.parent.parent and archivo.parent.parent.parent:
                anio, trimestre = extraer_anio_trimestre(archivo.parent.parent.parent)
            
            if anio != 0 and trimestre:
                # Detectar encoding y delimitador
                encoding = detectar_encoding(archivo)
                delimitador = detectar_delimitador(archivo, encoding)
                
                # Leer primeras líneas para validar columnas requeridas
                try:
                    # Leer el archivo solo para estadísticas
                    df = pd.read_csv(archivo, encoding=encoding, delimiter=delimitador, 
                                    nrows=10, low_memory=False)
                    columnas = len(df.columns)
                    
                    # Verificar columnas requeridas
                    columnas_requeridas = ["NOM_ENT", "NOM_MUN", "BP1_1"]
                    tiene_columnas_requeridas = all(col in df.columns for col in columnas_requeridas)
                    
                    if not tiene_columnas_requeridas:
                        columnas_faltantes = [col for col in columnas_requeridas if col not in df.columns]
                        logger.warning(f"Archivo {archivo} no tiene todas las columnas requeridas. Faltantes: {columnas_faltantes}")
                        continue
                    
                    # Contar filas sin cargar todo el archivo
                    filas = 0
                    for chunk in pd.read_csv(archivo, encoding=encoding, delimiter=delimitador, 
                                          chunksize=10000, low_memory=False):
                        filas += len(chunk)
                    
                    candidatos.append({
                        "origen": str(archivo),
                        "anio": anio,
                        "trimestre": trimestre,
                        "filas": filas,
                        "columnas": columnas,
                        "encoding": encoding,
                        "delimitador": delimitador,
                        "tiene_columnas_requeridas": True
                    })
                    
                    logger.info(f"Candidato válido encontrado: {archivo} - {anio}-{trimestre} ({filas} filas)")
                    
                except Exception as e:
                    logger.warning(f"Error al leer {archivo}: {str(e)}")
            else:
                logger.debug(f"No se pudo extraer año/trimestre de {archivo}")
        except Exception as e:
            logger.error(f"Error procesando archivo {archivo}: {str(e)}")
    
    # Ordenar por año y trimestre
    candidatos.sort(key=lambda x: (x["anio"], x["trimestre"]))
    
    logger.info(f"Total candidatos válidos encontrados: {len(candidatos)}")
    return candidatos


def buscar_diccionario(candidato: Dict[str, Any], logger: logging.Logger) -> Optional[str]:
    """
    Busca un diccionario asociado al candidato.
    
    Args:
        candidato: Diccionario con información del candidato
        logger: Logger para registrar eventos
        
    Returns:
        Ruta al diccionario si se encuentra, None en caso contrario
    """
    archivo_origen = Path(candidato["origen"])
    dir_padre = archivo_origen.parent
    
    # Buscar archivos de diccionario en el mismo directorio
    posibles_diccionarios = []
    
    # Patrones para identificar diccionarios
    palabras_clave = ['diccionario', 'dict', 'metadatos', 'meta']
    
    # Buscar en el directorio padre
    for archivo in dir_padre.glob('*.csv'):
        nombre_lower = str(archivo).lower()
        if any(palabra in nombre_lower for palabra in palabras_clave):
            posibles_diccionarios.append(archivo)
    
    # Si no encontramos, buscar en el directorio abuelo
    if not posibles_diccionarios and dir_padre.parent != dir_padre:
        for archivo in dir_padre.parent.glob('*.csv'):
            nombre_lower = str(archivo).lower()
            if any(palabra in nombre_lower for palabra in palabras_clave):
                posibles_diccionarios.append(archivo)
    
    # Devolver el primer diccionario encontrado
    if posibles_diccionarios:
        logger.info(f"Diccionario encontrado para {archivo_origen}: {posibles_diccionarios[0]}")
        return str(posibles_diccionarios[0])
    
    logger.debug(f"No se encontró diccionario para {archivo_origen}")
    return None


def procesar_candidatos(
    candidatos: List[Dict[str, Any]],
    output_dir: str,
    dryrun: bool,
    logger: logging.Logger
) -> List[Dict[str, Any]]:
    """
    Procesa los candidatos, copiándolos a la estructura estándar.
    
    Args:
        candidatos: Lista de candidatos a procesar
        output_dir: Directorio base de salida
        dryrun: Si es True, no copia archivos
        logger: Logger para registrar eventos
        
    Returns:
        Lista de inventario con información de archivos procesados
    """
    inventario = []
    
    # Crear directorios de salida
    raw_dir = os.path.join(output_dir, "insumos", "raw")
    dict_dir = os.path.join(output_dir, "insumos", "diccionarios")
    
    if not dryrun:
        os.makedirs(raw_dir, exist_ok=True)
        os.makedirs(dict_dir, exist_ok=True)
    
    logger.info(f"Procesando {len(candidatos)} candidatos...")
    
    # Procesar cada candidato
    for candidato in tqdm(candidatos, desc="Procesando candidatos"):
        try:
            anio = candidato["anio"]
            trimestre = candidato["trimestre"]
            
            # Definir nombres de archivos destino
            nombre_raw = f"ENSU_{anio}_{trimestre}_CB_raw.csv"
            ruta_raw = os.path.join(raw_dir, nombre_raw)
            
            # Buscar diccionario asociado
            ruta_dict_origen = buscar_diccionario(candidato, logger)
            ruta_dict_destino = None
            
            if ruta_dict_origen:
                nombre_dict = f"ENSU_{anio}_{trimestre}_diccionario.csv"
                ruta_dict_destino = os.path.join(dict_dir, nombre_dict)
            
            # Copiar archivos si no es dryrun
            if not dryrun:
                shutil.copy2(candidato["origen"], ruta_raw)
                logger.info(f"Archivo copiado: {candidato['origen']} -> {ruta_raw}")
                
                if ruta_dict_origen and os.path.exists(ruta_dict_origen):
                    shutil.copy2(ruta_dict_origen, ruta_dict_destino)
                    logger.info(f"Diccionario copiado: {ruta_dict_origen} -> {ruta_dict_destino}")
            
            # Actualizar inventario
            item_inventario = {
                "origen": candidato["origen"],
                "anio": anio,
                "trimestre": trimestre,
                "csv_raw": ruta_raw if not dryrun else candidato["origen"],
                "diccionario": ruta_dict_destino if ruta_dict_origen and not dryrun else ruta_dict_origen,
                "filas": candidato["filas"],
                "columnas": candidato["columnas"]
            }
            
            inventario.append(item_inventario)
            
        except Exception as e:
            logger.error(f"Error procesando candidato {candidato['origen']}: {str(e)}")
    
    return inventario


def guardar_inventario(
    inventario: List[Dict[str, Any]],
    output_dir: str,
    logger: logging.Logger
) -> str:
    """
    Guarda el inventario en formato JSON.
    
    Args:
        inventario: Lista con información del inventario
        output_dir: Directorio base de salida
        logger: Logger para registrar eventos
        
    Returns:
        Ruta al archivo de inventario
    """
    # Crear directorio para logs si no existe
    logs_dir = os.path.join(output_dir, "logs")
    os.makedirs(logs_dir, exist_ok=True)
    
    # Ruta del archivo de inventario
    inventario_path = os.path.join(logs_dir, "inventario_ensu.json")
    
    # Guardar inventario
    with open(inventario_path, 'w', encoding='utf-8') as f:
        json.dump(inventario, f, indent=2, ensure_ascii=False)
    
    logger.info(f"Inventario guardado en {inventario_path}")
    return inventario_path


def main():
    """Función principal del script."""
    parser = argparse.ArgumentParser(
        description="Orquestador de insumos ENSU del INEGI"
    )
    parser.add_argument(
        "--root",
        default=".\\datos completos",
        help="Directorio raíz donde buscar archivos ENSU"
    )
    parser.add_argument(
        "--out",
        default=".\\data\\yucatan-inseguridad",
        help="Directorio base de salida"
    )
    parser.add_argument(
        "--log",
        default=".\\data\\yucatan-inseguridad\\logs\\orquestador.log",
        help="Ruta al archivo de log"
    )
    parser.add_argument(
        "--dryrun",
        action="store_true",
        help="Modo simulación (no copia archivos)"
    )
    
    args = parser.parse_args()
    
    # Configurar logging
    logger = setup_logging(args.log)
    
    try:
        logger.info("Iniciando orquestador ENSU")
        logger.info(f"Directorio raíz: {args.root}")
        logger.info(f"Directorio salida: {args.out}")
        logger.info(f"Modo dryrun: {args.dryrun}")
        
        # Verificar que exista el directorio raíz
        if not os.path.exists(args.root):
            logger.error(f"El directorio raíz '{args.root}' no existe")
            sys.exit(1)
        
        # Buscar candidatos
        candidatos = buscar_candidatos_ensu(args.root, logger)
        
        if not candidatos:
            logger.warning("No se encontraron candidatos ENSU")
            sys.exit(0)
        
        # Procesar candidatos
        inventario = procesar_candidatos(candidatos, args.out, args.dryrun, logger)
        
        # Guardar inventario
        ruta_inventario = guardar_inventario(inventario, args.out, logger)
        
        # Imprimir resumen
        periodos_unicos = set(f"{item['anio']}-{item['trimestre']}" for item in inventario)
        
        print("\n--- RESUMEN ---")
        print(f"Paquetes detectados: {len(candidatos)}")
        print(f"Periodos únicos: {len(periodos_unicos)} ({', '.join(sorted(periodos_unicos))})")
        print(f"Total archivos {'analizados' if args.dryrun else 'copiados'}: {len(inventario)}")
        print(f"Inventario guardado en: {ruta_inventario}")
        
        logger.info("Orquestador finalizado exitosamente")
        
    except Exception as e:
        logger.exception(f"Error en el orquestador: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
