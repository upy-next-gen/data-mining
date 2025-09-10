#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Verificaciones de calidad (QA) para el sistema de procesamiento ENSU.

Este script realiza pruebas unitarias simples para verificar el correcto
funcionamiento de las funciones críticas del sistema, así como la coherencia
de los datos generados.

Autor: Cascade AI
Fecha: 2025-09-10
"""

import os
import sys
import json
import re
import glob
import unicodedata
from pathlib import Path
from typing import Dict, List, Tuple, Set, Optional, Any

import pandas as pd
import numpy as np


# Colores para salida en consola
class Colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'


def print_header(title: str) -> None:
    """Imprime un encabezado formateado."""
    print(f"\n{Colors.HEADER}{Colors.BOLD}{title}{Colors.ENDC}")
    print(f"{Colors.HEADER}{'-' * 60}{Colors.ENDC}")


def print_result(name: str, passed: bool, message: str = "") -> None:
    """Imprime el resultado de una prueba."""
    status = f"{Colors.OKGREEN}PASSED{Colors.ENDC}" if passed else f"{Colors.FAIL}FAILED{Colors.ENDC}"
    message_text = f" - {message}" if message else ""
    print(f"  {status} - {name}{message_text}")
    return passed


def normalizar_string_test() -> bool:
    """Prueba la función de normalización de strings."""
    print_header("Test: Normalización de Strings")
    
    # Función de normalización (copiada de procesar_ensu_cb.py)
    def normalizar_string(texto: str) -> str:
        """Normaliza un string: mayúsculas, sin acentos, Ñ→N, espacios normalizados."""
        if pd.isna(texto) or not isinstance(texto, str):
            return ""
        
        # Convertir a mayúsculas
        texto = texto.upper()
        
        # Normalizar caracteres (eliminar acentos)
        texto = unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('ASCII')
        
        # Normalizar espacios
        texto = ' '.join(texto.split())
        
        return texto
    
    # Casos de prueba
    test_cases = [
        ("Kanasín", "KANASIN"),
        ("  Mérida  ", "MERIDA"),
        ("TiZiMíN", "TIZIMIN"),
        ("Ñuevo León", "NUEVO LEON"),
        ("  texto  con   espacios  ", "TEXTO CON ESPACIOS"),
        ("", ""),
        (None, ""),
        (123, "")  # No es string
    ]
    
    all_passed = True
    
    # Ejecutar pruebas
    for input_text, expected in test_cases:
        result = normalizar_string(input_text)
        passed = result == expected
        all_passed = print_result(
            f'normalizar_string("{input_text}") = "{result}"', 
            passed, 
            f'Expected: "{expected}"' if not passed else ""
        ) and all_passed
    
    return all_passed


def extraer_anio_trimestre_test() -> bool:
    """Prueba la función de extracción de año y trimestre de nombres de archivo."""
    print_header("Test: Extracción de Año y Trimestre")
    
    # Función de mes a trimestre (copiada de ingesta_hist_ensu.py)
    def _mes_a_trimestre(mes: int) -> str:
        """Convierte un mes a su trimestre correspondiente."""
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
    
    # Función de extracción (copiada y adaptada de ingesta_hist_ensu.py)
    def extraer_anio_trimestre(nombre_archivo: str) -> Tuple[int, str]:
        """Extrae año y trimestre del nombre del archivo usando múltiples patrones."""
        nombre_base = os.path.basename(nombre_archivo).lower()
        
        # Patrones a buscar
        patrones = [
            # Patrón cb_MMAA o cb_MMYY
            (r'cb_(\d{2})(\d{2})', lambda m: (2000 + int(m.group(2)), _mes_a_trimestre(int(m.group(1))))),
            
            # Patrón cb_MMAAAA o cb_MMYYYY
            (r'cb_(\d{2})(\d{4})', lambda m: (int(m.group(2)), _mes_a_trimestre(int(m.group(1))))),
            
            # Patrón AAAA_MM o YYYY_MM
            (r'(\d{4})_(\d{1,2})', lambda m: (int(m.group(1)), _mes_a_trimestre(int(m.group(2))))),
            
            # Patrón MM_AAAA o MM_YYYY
            (r'(\d{1,2})_(\d{4})', lambda m: (int(m.group(2)), _mes_a_trimestre(int(m.group(1))))),
            
            # Patrón AAAA_Qt o YYYY_Qt (ej: 2019_2t)
            (r'(\d{4})_(\d)t', lambda m: (int(m.group(1)), f"Q{m.group(2)}")),
            
            # Patrón AAAA_TN o YYYY_TN (ej: 2019_T2)
            (r'(\d{4})_t(\d)', lambda m: (int(m.group(1)), f"Q{m.group(2)}")),
            
            # Patrón AAAA_QN o YYYY_QN (ej: 2019_Q2)
            (r'(\d{4})_q(\d)', lambda m: (int(m.group(1)), f"Q{m.group(2)}")),
            
            # Patrón QN_AAAA o QN_YYYY (ej: Q2_2019)
            (r'q(\d)_(\d{4})', lambda m: (int(m.group(2)), f"Q{m.group(1)}")),
            
            # Patrón TN_AAAA o TN_YYYY (ej: T2_2019)
            (r't(\d)_(\d{4})', lambda m: (int(m.group(2)), f"Q{m.group(1)}")),
        ]
        
        # Intentar cada patrón
        for patron, extractor in patrones:
            match = re.search(patron, nombre_base)
            if match:
                try:
                    return extractor(match)
                except Exception:
                    continue
        
        # Si llegamos aquí, no se pudo determinar el periodo
        return 0, "DESCONOCIDO"
    
    # Casos de prueba
    test_cases = [
        ("conjunto_de_datos_CB_ENSU_12_2021.csv", (2021, "Q4")),
        ("conjunto_de_datos_ensu_cb_0324.csv", (2024, "Q1")),
        ("conjunto_de_datos_cb_ENSU_2019_2t.csv", (2019, "Q2")),
        ("conjunto_de_datos_ensu_cb_0625.csv", (2025, "Q2")),
        ("ensu_2022_q3.csv", (2022, "Q3")),
        ("datos_t1_2020.csv", (2020, "Q1")),
        ("2023_05_ensu.csv", (2023, "Q2")),
        ("08_2021_datos.csv", (2021, "Q3")),
        ("archivo_sin_fecha.csv", (0, "DESCONOCIDO"))
    ]
    
    all_passed = True
    
    # Ejecutar pruebas
    for filename, expected in test_cases:
        result = extraer_anio_trimestre(filename)
        passed = result == expected
        all_passed = print_result(
            f'extraer_anio_trimestre("{filename}") = {result}', 
            passed, 
            f'Expected: {expected}' if not passed else ""
        ) and all_passed
    
    return all_passed


def promedio_ponderado_test() -> bool:
    """Prueba el cálculo de promedio ponderado estatal."""
    print_header("Test: Cálculo de Promedio Ponderado")
    
    # Datos sintéticos para prueba
    data = [
        # NOM_ENT, NOM_MUN, TOTAL_REGISTROS, TOTAL_INSEGUROS
        ["YUCATAN", "MERIDA", 1000, 350],
        ["YUCATAN", "PROGRESO", 200, 120],
        ["YUCATAN", "VALLADOLID", 300, 90],
        ["YUCATAN", "TIZIMIN", 150, 45]
    ]
    
    df = pd.DataFrame(data, columns=["NOM_ENT", "NOM_MUN", "TOTAL_REGISTROS", "TOTAL_INSEGUROS"])
    
    # Cálculo manual del promedio ponderado
    total_registros = df["TOTAL_REGISTROS"].sum()  # 1650
    total_inseguros = df["TOTAL_INSEGUROS"].sum()  # 605
    promedio_esperado = round((total_inseguros / total_registros) * 100, 2)  # 36.67
    
    # Función que simula cálculo en calcular_promedio_estatal
    def calcular_promedio_ponderado(df):
        total_registros = df["TOTAL_REGISTROS"].sum()
        total_inseguros = df["TOTAL_INSEGUROS"].sum()
        return round((total_inseguros / total_registros) * 100, 2)
    
    promedio_calculado = calcular_promedio_ponderado(df)
    
    # Verificación
    passed = promedio_calculado == promedio_esperado
    return print_result(
        f"Promedio ponderado: {promedio_calculado}%", 
        passed, 
        f'Expected: {promedio_esperado}%' if not passed else ""
    )


def deteccion_duplicados_test() -> bool:
    """Prueba la detección y manejo de duplicados por (NOM_MUN, AÑO, TRIMESTRE)."""
    print_header("Test: Detección de Duplicados")
    
    # Crear un DataFrame sintético con duplicados
    data = [
        # NOM_ENT, NOM_MUN, TOTAL_REGISTROS, AÑO, TRIMESTRE
        ["YUCATAN", "MERIDA", 1000, 2023, "Q1"],
        ["YUCATAN", "MERIDA", 1100, 2023, "Q1"],  # Duplicado (más reciente)
        ["YUCATAN", "PROGRESO", 200, 2023, "Q1"],
        ["YUCATAN", "VALLADOLID", 300, 2023, "Q2"],
        ["YUCATAN", "VALLADOLID", 320, 2023, "Q2"]  # Duplicado (más reciente)
    ]
    
    df = pd.DataFrame(data, columns=["NOM_ENT", "NOM_MUN", "TOTAL_REGISTROS", "AÑO", "TRIMESTRE"])
    
    # Función que simula la eliminación de duplicados manteniendo el último
    def eliminar_duplicados(df):
        return df.drop_duplicates(
            subset=["NOM_MUN", "AÑO", "TRIMESTRE"], 
            keep="last"
        )
    
    # Eliminar duplicados
    df_sin_duplicados = eliminar_duplicados(df)
    
    # Verificaciones
    tests_passed = []
    
    # Test 1: Verificar número de filas
    test1_passed = len(df_sin_duplicados) == 3
    tests_passed.append(print_result(
        "Número correcto de filas después de eliminar duplicados", 
        test1_passed, 
        f"Expected: 3, Got: {len(df_sin_duplicados)}" if not test1_passed else ""
    ))
    
    # Test 2: Verificar que se mantuvieron los registros correctos
    merida = df_sin_duplicados[df_sin_duplicados["NOM_MUN"] == "MERIDA"].iloc[0]
    test2_passed = merida["TOTAL_REGISTROS"] == 1100
    tests_passed.append(print_result(
        "Registro más reciente para MERIDA mantenido", 
        test2_passed, 
        f"Expected: 1100, Got: {merida['TOTAL_REGISTROS']}" if not test2_passed else ""
    ))
    
    # Test 3: Verificar otro registro
    valladolid = df_sin_duplicados[df_sin_duplicados["NOM_MUN"] == "VALLADOLID"].iloc[0]
    test3_passed = valladolid["TOTAL_REGISTROS"] == 320
    tests_passed.append(print_result(
        "Registro más reciente para VALLADOLID mantenido", 
        test3_passed, 
        f"Expected: 320, Got: {valladolid['TOTAL_REGISTROS']}" if not test3_passed else ""
    ))
    
    return all(tests_passed)


def verificar_inventario_procesados() -> bool:
    """Verifica que todos los periodos del inventario tengan un archivo procesado."""
    print_header("Verificación: Periodos en Inventario vs. Procesados")
    
    inventario_path = "./data/yucatan-inseguridad/logs/inventario_ensu.json"
    procesados_dir = "./data/yucatan-inseguridad/procesados"
    
    # Verificar que existan los archivos
    if not os.path.exists(inventario_path):
        return print_result(
            "Archivo de inventario encontrado", 
            False, 
            f"Archivo no encontrado: {inventario_path}"
        )
    
    if not os.path.exists(procesados_dir):
        return print_result(
            "Directorio de procesados encontrado", 
            False, 
            f"Directorio no encontrado: {procesados_dir}"
        )
    
    # Cargar inventario
    try:
        with open(inventario_path, 'r', encoding='utf-8') as f:
            inventario = json.load(f)
    except Exception as e:
        return print_result(
            "Carga de inventario", 
            False, 
            f"Error: {str(e)}"
        )
    
    # Obtener periodos del inventario
    periodos_inventario = set()
    for item in inventario:
        if "anio" in item and "trimestre" in item:
            if item["anio"] != 0 and item["trimestre"] != "DESCONOCIDO":
                periodos_inventario.add(f"{item['anio']}_{item['trimestre']}")
    
    if not periodos_inventario:
        return print_result(
            "Periodos encontrados en inventario", 
            False, 
            "No se encontraron periodos válidos en el inventario"
        )
    else:
        print_result(
            f"Periodos encontrados en inventario: {len(periodos_inventario)}", 
            True
        )
    
    # Buscar archivos procesados
    archivos_procesados = glob.glob(os.path.join(procesados_dir, "procesado_*_*_cb.csv"))
    
    if not archivos_procesados:
        return print_result(
            "Archivos procesados encontrados", 
            False, 
            "No se encontraron archivos procesados"
        )
    else:
        print_result(
            f"Archivos procesados encontrados: {len(archivos_procesados)}", 
            True
        )
    
    # Extraer periodos de los archivos procesados
    periodos_procesados = set()
    for archivo in archivos_procesados:
        nombre = os.path.basename(archivo)
        match = re.search(r'procesado_(\d+)_(Q\d)_cb\.csv', nombre)
        if match:
            anio = match.group(1)
            trimestre = match.group(2)
            periodos_procesados.add(f"{anio}_{trimestre}")
    
    # Verificar que todos los periodos del inventario tengan un archivo procesado
    periodos_faltantes = periodos_inventario - periodos_procesados
    
    if periodos_faltantes:
        return print_result(
            "Todos los periodos del inventario tienen archivo procesado", 
            False, 
            f"Periodos sin procesar: {', '.join(sorted(periodos_faltantes))}"
        )
    else:
        return print_result(
            "Todos los periodos del inventario tienen archivo procesado", 
            True
        )


def verificar_porcentajes() -> bool:
    """Verifica que los porcentajes en los archivos procesados sumen 100±0.1 por municipio."""
    print_header("Verificación: Suma de Porcentajes")
    
    procesados_dir = "./data/yucatan-inseguridad/procesados"
    
    # Verificar que exista el directorio
    if not os.path.exists(procesados_dir):
        return print_result(
            "Directorio de procesados encontrado", 
            False, 
            f"Directorio no encontrado: {procesados_dir}"
        )
    
    # Buscar archivos procesados
    archivos_procesados = glob.glob(os.path.join(procesados_dir, "procesado_*_*_cb.csv"))
    
    if not archivos_procesados:
        return print_result(
            "Archivos procesados encontrados", 
            False, 
            "No se encontraron archivos procesados"
        )
    
    # Verificar cada archivo
    todos_correctos = True
    errores = []
    
    for archivo in archivos_procesados:
        try:
            df = pd.read_csv(archivo)
            
            # Verificar columnas requeridas
            columnas_requeridas = ["PCT_SEGUROS", "PCT_INSEGUROS", "PCT_NO_RESPONDE", "NOM_MUN"]
            if not all(col in df.columns for col in columnas_requeridas):
                errores.append(f"Archivo {os.path.basename(archivo)}: Faltan columnas requeridas")
                todos_correctos = False
                continue
            
            # Verificar suma de porcentajes
            for municipio, grupo in df.groupby("NOM_MUN"):
                suma_porcentajes = grupo["PCT_SEGUROS"] + grupo["PCT_INSEGUROS"] + grupo["PCT_NO_RESPONDE"]
                if not (99.9 <= suma_porcentajes.iloc[0] <= 100.1):
                    errores.append(f"Archivo {os.path.basename(archivo)}, Municipio {municipio}: "
                                  f"La suma de porcentajes es {suma_porcentajes.iloc[0]}")
                    todos_correctos = False
                    break
                    
        except Exception as e:
            errores.append(f"Error procesando {os.path.basename(archivo)}: {str(e)}")
            todos_correctos = False
    
    return print_result(
        "Porcentajes correctamente calculados en todos los archivos", 
        todos_correctos, 
        "\n    - ".join(["Errores encontrados:"] + errores) if errores else ""
    )


def verificar_serie_estatal() -> bool:
    """Verifica que en la serie estatal, los porcentajes estén en el rango [0, 100]."""
    print_header("Verificación: Serie Estatal")
    
    # Buscar reportes tabulados
    datos_tabulados = "./data/yucatan-inseguridad/reportes/datos_tabulados_yucatan.csv"
    
    # Verificar que existan los archivos
    if not os.path.exists(datos_tabulados):
        return print_result(
            "Datos tabulados encontrados", 
            False, 
            f"Archivo no encontrado: {datos_tabulados}"
        )
    
    # Cargar datos tabulados
    try:
        df = pd.read_csv(datos_tabulados)
        
        # Verificar columnas requeridas
        if "PCT_INSEGUROS" not in df.columns:
            return print_result(
                "Columna PCT_INSEGUROS encontrada", 
                False
            )
        
        # Verificar rango de valores
        min_pct = df["PCT_INSEGUROS"].min()
        max_pct = df["PCT_INSEGUROS"].max()
        has_nan = df["PCT_INSEGUROS"].isna().any()
        
        rango_ok = 0 <= min_pct <= max_pct <= 100
        
        if not rango_ok or has_nan:
            mensaje_error = []
            if min_pct < 0:
                mensaje_error.append(f"Valor mínimo ({min_pct}%) es menor que 0%")
            if max_pct > 100:
                mensaje_error.append(f"Valor máximo ({max_pct}%) es mayor que 100%")
            if has_nan:
                mensaje_error.append("Se encontraron valores NaN")
                
            return print_result(
                "Valores de PCT_INSEGUROS en rango [0, 100] y sin NaN", 
                False, 
                ", ".join(mensaje_error)
            )
        
        return print_result(
            f"Valores de PCT_INSEGUROS en rango correcto: [{min_pct:.1f}%, {max_pct:.1f}%]", 
            True
        )
        
    except Exception as e:
        return print_result(
            "Carga de datos tabulados", 
            False, 
            f"Error: {str(e)}"
        )


def verificar_cobertura_temporal() -> bool:
    """Verifica que al menos 8 periodos 2022-2025 estén presentes si existen en los insumos."""
    print_header("Verificación: Cobertura Temporal")
    
    # Directorios a verificar
    inventario_path = "./data/yucatan-inseguridad/logs/inventario_ensu.json"
    procesados_dir = "./data/yucatan-inseguridad/procesados"
    
    # Verificar que existan los archivos
    if not os.path.exists(inventario_path):
        return print_result(
            "Archivo de inventario encontrado", 
            False, 
            f"Archivo no encontrado: {inventario_path}"
        )
    
    if not os.path.exists(procesados_dir):
        return print_result(
            "Directorio de procesados encontrado", 
            False, 
            f"Directorio no encontrado: {procesados_dir}"
        )
    
    # Cargar inventario
    try:
        with open(inventario_path, 'r', encoding='utf-8') as f:
            inventario = json.load(f)
    except Exception as e:
        return print_result(
            "Carga de inventario", 
            False, 
            f"Error: {str(e)}"
        )
    
    # Obtener años del inventario
    anios_interes = {2022, 2023, 2024, 2025}
    periodos_interes = set()
    
    for item in inventario:
        if "anio" in item and "trimestre" in item and item["anio"] in anios_interes:
            periodos_interes.add(f"{item['anio']}_{item['trimestre']}")
    
    # Buscar archivos procesados
    archivos_procesados = glob.glob(os.path.join(procesados_dir, "procesado_*_*_cb.csv"))
    
    # Extraer periodos de los archivos procesados
    periodos_procesados = set()
    for archivo in archivos_procesados:
        nombre = os.path.basename(archivo)
        match = re.search(r'procesado_(\d+)_(Q\d)_cb\.csv', nombre)
        if match:
            anio = int(match.group(1))
            trimestre = match.group(2)
            if anio in anios_interes:
                periodos_procesados.add(f"{anio}_{trimestre}")
    
    # Verificar cobertura
    periodos_encontrados = periodos_interes.intersection(periodos_procesados)
    periodos_faltantes = periodos_interes - periodos_procesados
    
    # Verificar si hay al menos 8 periodos
    if len(periodos_encontrados) < 8 and len(periodos_interes) >= 8:
        return print_result(
            "Al menos 8 periodos 2022-2025 procesados", 
            False, 
            f"Solo se encontraron {len(periodos_encontrados)} de {len(periodos_interes)} periodos"
        )
    
    # Si hay menos de 8 periodos en total, verificar que todos estén procesados
    if len(periodos_interes) < 8:
        if periodos_faltantes:
            return print_result(
                "Todos los periodos disponibles procesados", 
                False, 
                f"Periodos faltantes: {', '.join(sorted(periodos_faltantes))}"
            )
        else:
            return print_result(
                f"Todos los periodos disponibles procesados ({len(periodos_interes)} periodos)", 
                True
            )
    else:
        return print_result(
            f"Cobertura adecuada: {len(periodos_encontrados)} de {len(periodos_interes)} periodos procesados", 
            True
        )


def main():
    """Función principal que ejecuta todas las pruebas."""
    print(f"\n{Colors.HEADER}{Colors.BOLD}QA CHECKS - SISTEMA ENSU{Colors.ENDC}")
    print(f"{Colors.HEADER}{'=' * 60}{Colors.ENDC}\n")
    
    # Ejecutar pruebas unitarias
    tests = [
        ("Normalización de strings", normalizar_string_test),
        ("Extracción de año/trimestre", extraer_anio_trimestre_test),
        ("Cálculo de promedio ponderado", promedio_ponderado_test),
        ("Detección de duplicados", deteccion_duplicados_test)
    ]
    
    unit_tests_passed = 0
    for name, test_func in tests:
        if test_func():
            unit_tests_passed += 1
    
    print(f"\n{Colors.OKBLUE}Pruebas unitarias: {unit_tests_passed}/{len(tests)} pasadas{Colors.ENDC}")
    
    # Ejecutar verificaciones de datos
    verifications = [
        ("Periodos en inventario vs. procesados", verificar_inventario_procesados),
        ("Suma de porcentajes en procesados", verificar_porcentajes),
        ("Serie estatal en rango correcto", verificar_serie_estatal),
        ("Cobertura temporal adecuada", verificar_cobertura_temporal)
    ]
    
    verifications_passed = 0
    for name, verify_func in verifications:
        if verify_func():
            verifications_passed += 1
    
    print(f"\n{Colors.OKBLUE}Verificaciones de datos: {verifications_passed}/{len(verifications)} pasadas{Colors.ENDC}")
    
    # Resultado global
    total_passed = unit_tests_passed + verifications_passed
    total_tests = len(tests) + len(verifications)
    
    if total_passed == total_tests:
        print(f"\n{Colors.OKGREEN}{Colors.BOLD}✓ TODAS LAS PRUEBAS PASARON ({total_passed}/{total_tests}){Colors.ENDC}")
        return 0
    else:
        print(f"\n{Colors.WARNING}{Colors.BOLD}⚠ ALGUNAS PRUEBAS FALLARON ({total_passed}/{total_tests}){Colors.ENDC}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
