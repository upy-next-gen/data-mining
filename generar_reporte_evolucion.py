#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Generador de reportes de evolución de percepción de seguridad en Yucatán.

Este script genera un reporte HTML con visualizaciones que muestran la evolución
temporal del porcentaje de percepción de inseguridad en el estado de Yucatán
y sus municipios.

Autor: Cascade AI
Fecha: 2025-09-10
"""

import os
import sys
import re
import glob
import json
import logging
import argparse
import base64
from io import BytesIO
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Set, Optional, Union, Any

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')  # No usar interfaz gráfica
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator


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
    logger = logging.getLogger("reporte_evolucion")
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


def cargar_datos_procesados(directorio_procesados: str, estado: str, logger: logging.Logger) -> pd.DataFrame:
    """
    Carga todos los archivos CSV procesados y los combina en un solo DataFrame.
    
    Args:
        directorio_procesados: Directorio con los archivos procesados
        estado: Estado a filtrar (ya normalizado)
        logger: Logger para registrar eventos
        
    Returns:
        DataFrame combinado con todos los datos
    """
    # Buscar archivos CSV procesados
    patron_archivos = os.path.join(directorio_procesados, "procesados", "procesado_*.csv")
    archivos = glob.glob(patron_archivos)
    
    if not archivos:
        logger.error(f"No se encontraron archivos procesados en {directorio_procesados}/procesados/")
        sys.exit(1)
    
    logger.info(f"Se encontraron {len(archivos)} archivos procesados")
    
    # Cargar y combinar archivos
    dfs = []
    for archivo in archivos:
        try:
            df = pd.read_csv(archivo)
            dfs.append(df)
            logger.debug(f"Archivo cargado: {archivo}")
        except Exception as e:
            logger.error(f"Error al cargar {archivo}: {str(e)}")
    
    if not dfs:
        logger.error("No se pudo cargar ningún archivo de datos")
        sys.exit(1)
    
    # Combinar todos los DataFrames
    df_combinado = pd.concat(dfs, ignore_index=True)
    
    # Verificar que contenga los datos esperados
    if len(df_combinado) == 0:
        logger.error("No hay datos en los archivos procesados")
        sys.exit(1)
    
    # Filtrar por estado (aunque debería venir ya filtrado)
    df_filtrado = df_combinado[df_combinado["NOM_ENT"] == estado]
    
    if len(df_filtrado) == 0:
        logger.error(f"No hay datos para el estado '{estado}'")
        sys.exit(1)
    
    # Asegurar que PERIODO esté presente
    if "PERIODO" not in df_filtrado.columns:
        df_filtrado["PERIODO"] = df_filtrado["AÑO"].astype(str) + "-" + df_filtrado["TRIMESTRE"]
        logger.info("Columna PERIODO generada automáticamente")
    
    # Orden natural de periodos
    periodos_ordenados = sorted(df_filtrado["PERIODO"].unique(), 
                             key=lambda x: (int(x.split("-")[0]), x.split("-")[1]))
    
    # Crear mapeo de periodo a índice numérico para ordenamiento
    mapeo_periodos = {periodo: i for i, periodo in enumerate(periodos_ordenados)}
    df_filtrado["INDICE_PERIODO"] = df_filtrado["PERIODO"].map(mapeo_periodos)
    
    # Crear columna de año-trimestre para ordenar correctamente
    df_filtrado["ORDEN_AÑO_TRIMESTRE"] = df_filtrado.apply(
        lambda x: f"{x['AÑO']:04d}-{x['TRIMESTRE']}", axis=1
    )
    
    logger.info(f"Datos cargados: {len(df_filtrado)} registros para {len(df_filtrado['NOM_MUN'].unique())} municipios")
    
    return df_filtrado


def calcular_promedio_estatal(df: pd.DataFrame, logger: logging.Logger) -> Tuple[pd.DataFrame, float]:
    """
    Calcula el promedio estatal ponderado por periodo.
    
    Args:
        df: DataFrame con datos filtrados por estado
        logger: Logger para registrar eventos
        
    Returns:
        Tuple con (DataFrame con promedios por periodo, promedio histórico)
    """
    # Agrupar por periodo
    periodos_ordenados = sorted(df["PERIODO"].unique(), 
                             key=lambda x: (int(x.split("-")[0]), x.split("-")[1]))
    
    # Crear DataFrame para almacenar promedios estatales
    promedios_estatales = []
    
    # Calcular promedio estatal por periodo (ponderado por TOTAL_REGISTROS)
    for periodo in periodos_ordenados:
        df_periodo = df[df["PERIODO"] == periodo]
        total_registros = df_periodo["TOTAL_REGISTROS"].sum()
        total_inseguros = df_periodo["TOTAL_INSEGUROS"].sum()
        
        # Calcular porcentaje ponderado
        if total_registros > 0:
            pct_inseguros = (total_inseguros / total_registros) * 100
        else:
            pct_inseguros = 0
        
        promedios_estatales.append({
            "PERIODO": periodo,
            "AÑO": df_periodo["AÑO"].iloc[0],
            "TRIMESTRE": df_periodo["TRIMESTRE"].iloc[0],
            "TOTAL_REGISTROS_ESTATAL": total_registros,
            "TOTAL_INSEGUROS_ESTATAL": total_inseguros,
            "PCT_INSEGUROS_ESTATAL": round(pct_inseguros, 2)
        })
    
    # Crear DataFrame
    df_estatal = pd.DataFrame(promedios_estatales)
    
    # Calcular promedio histórico ponderado
    total_registros_historico = df["TOTAL_REGISTROS"].sum()
    total_inseguros_historico = df["TOTAL_INSEGUROS"].sum()
    
    if total_registros_historico > 0:
        promedio_historico = round((total_inseguros_historico / total_registros_historico) * 100, 2)
    else:
        promedio_historico = 0
    
    logger.info(f"Promedio histórico estatal: {promedio_historico}%")
    
    return df_estatal, promedio_historico


def identificar_periodos_municipios(df: pd.DataFrame) -> Tuple[List[str], Dict[str, Set[str]], Dict[str, int]]:
    """
    Identifica los periodos disponibles y cuáles están presentes para cada municipio.
    
    Args:
        df: DataFrame con datos filtrados por estado
        
    Returns:
        Tuple con (lista de periodos, diccionario de periodos por municipio, conteo de periodos por municipio)
    """
    # Obtener periodos ordenados
    periodos = sorted(df["PERIODO"].unique(), 
                   key=lambda x: (int(x.split("-")[0]), x.split("-")[1]))
    
    # Crear diccionario para almacenar periodos por municipio
    periodos_por_municipio = {}
    conteo_periodos = {}
    
    # Identificar periodos presentes para cada municipio
    for municipio in df["NOM_MUN"].unique():
        df_muni = df[df["NOM_MUN"] == municipio]
        periodos_disponibles = set(df_muni["PERIODO"])
        periodos_por_municipio[municipio] = periodos_disponibles
        conteo_periodos[municipio] = len(periodos_disponibles)
    
    return periodos, periodos_por_municipio, conteo_periodos


def generar_grafico_estatal(
    df_estatal: pd.DataFrame, 
    promedio_historico: float
) -> str:
    """
    Genera un gráfico de línea con la evolución estatal de percepción de inseguridad.
    
    Args:
        df_estatal: DataFrame con promedios estatales por periodo
        promedio_historico: Valor del promedio histórico estatal
        
    Returns:
        String con imagen codificada en base64
    """
    # Crear figura
    plt.figure(figsize=(10, 5))
    
    # Obtener datos
    periodos = df_estatal["PERIODO"].tolist()
    valores = df_estatal["PCT_INSEGUROS_ESTATAL"].tolist()
    
    # Definir colores según niveles
    colores = ['green' if v < 30 else 'orange' if v < 60 else 'red' for v in valores]
    
    # Crear gráfico de línea
    plt.plot(periodos, valores, marker='o', linestyle='-', color='blue', linewidth=2, zorder=5)
    
    # Añadir puntos con color según nivel
    for i, (periodo, valor) in enumerate(zip(periodos, valores)):
        plt.scatter(periodo, valor, color=colores[i], s=80, zorder=10)
    
    # Añadir línea de promedio histórico
    plt.axhline(y=promedio_historico, color='gray', linestyle='--', alpha=0.7, 
              label=f'Promedio histórico: {promedio_historico}%')
    
    # Añadir etiquetas y título
    plt.title("Evolución de Percepción de Inseguridad en Yucatán", fontsize=14, fontweight='bold')
    plt.xlabel("Periodo", fontsize=12)
    plt.ylabel("% Percepción de Inseguridad", fontsize=12)
    
    # Añadir grid
    plt.grid(True, alpha=0.3, linestyle='--')
    
    # Añadir leyenda
    plt.legend(loc='best')
    
    # Rotar etiquetas del eje X
    plt.xticks(rotation=45, ha='right')
    
    # Añadir límites y márgenes
    plt.ylim(0, 100)
    plt.tight_layout()
    
    # Guardar imagen en memoria
    buffer = BytesIO()
    plt.savefig(buffer, format='png', dpi=100)
    plt.close()
    
    # Codificar imagen en base64
    buffer.seek(0)
    imagen_base64 = base64.b64encode(buffer.read()).decode('utf-8')
    
    return imagen_base64


def generar_grafico_municipio(
    df: pd.DataFrame, 
    municipio: str,
    periodos: List[str],
    periodos_municipio: Set[str]
) -> Tuple[str, float, int]:
    """
    Genera un gráfico para un municipio específico.
    
    Args:
        df: DataFrame con datos filtrados por estado
        municipio: Nombre del municipio
        periodos: Lista completa de periodos
        periodos_municipio: Set con periodos disponibles para el municipio
        
    Returns:
        Tuple con (imagen base64, promedio histórico, número de periodos)
    """
    # Filtrar datos para el municipio
    df_muni = df[df["NOM_MUN"] == municipio]
    
    # Crear figura (tamaño más pequeño para municipios)
    plt.figure(figsize=(8, 4))
    
    # Crear arrays con valores para todos los periodos (incluyendo huecos)
    valores = []
    periodos_grafico = []
    colores = []
    
    # Calcular promedio histórico para el municipio
    total_registros = df_muni["TOTAL_REGISTROS"].sum()
    total_inseguros = df_muni["TOTAL_INSEGUROS"].sum()
    
    if total_registros > 0:
        promedio_historico = round((total_inseguros / total_registros) * 100, 2)
    else:
        promedio_historico = 0
    
    # Para cada periodo, verificar si hay datos para el municipio
    for periodo in periodos:
        if periodo in periodos_municipio:
            # Hay datos para este periodo
            valor = df_muni[df_muni["PERIODO"] == periodo]["PCT_INSEGUROS"].iloc[0]
            valores.append(valor)
            periodos_grafico.append(periodo)
            # Color según nivel de inseguridad
            colores.append('green' if valor < 30 else 'orange' if valor < 60 else 'red')
    
    # Si no hay suficientes datos, mostrar mensaje
    if len(valores) < 2:
        plt.text(0.5, 0.5, f"Datos insuficientes para {municipio}", 
               ha='center', va='center', transform=plt.gca().transAxes)
    else:
        # Crear gráfico de línea
        plt.plot(periodos_grafico, valores, marker='o', linestyle='-', color='blue', linewidth=2, zorder=5)
        
        # Añadir puntos con color según nivel
        for i, (periodo, valor) in enumerate(zip(periodos_grafico, valores)):
            plt.scatter(periodo, valor, color=colores[i], s=60, zorder=10)
        
        # Añadir línea de promedio histórico
        plt.axhline(y=promedio_historico, color='gray', linestyle='--', alpha=0.7, 
                  label=f'Prom: {promedio_historico}%')
        
        # Añadir etiquetas
        plt.ylabel("% Inseguridad", fontsize=10)
        
        # Añadir grid
        plt.grid(True, alpha=0.3, linestyle=':')
        
        # Añadir leyenda
        plt.legend(loc='best', fontsize=8)
    
    # Añadir título
    plt.title(f"{municipio}", fontsize=12, fontweight='bold')
    
    # Rotar etiquetas del eje X
    plt.xticks(rotation=45, ha='right', fontsize=8)
    
    # Añadir límites y márgenes
    plt.ylim(0, 100)
    plt.tight_layout()
    
    # Guardar imagen en memoria
    buffer = BytesIO()
    plt.savefig(buffer, format='png', dpi=100)
    plt.close()
    
    # Codificar imagen en base64
    buffer.seek(0)
    imagen_base64 = base64.b64encode(buffer.read()).decode('utf-8')
    
    return imagen_base64, promedio_historico, len(periodos_municipio)


def generar_tabla_pivote(df: pd.DataFrame, periodos: List[str]) -> str:
    """
    Genera una tabla pivote con porcentajes de inseguridad por municipio y periodo.
    
    Args:
        df: DataFrame con datos filtrados por estado
        periodos: Lista completa de periodos ordenados
        
    Returns:
        HTML de la tabla pivote
    """
    # Crear pivot table: municipios vs periodos
    pivot = pd.pivot_table(
        df,
        values='PCT_INSEGUROS',
        index=['NOM_MUN'],
        columns=['PERIODO'],
        aggfunc='first',  # Tomar el primer valor (ya están agregados)
        fill_value=None
    )
    
    # Reordenar columnas según periodos
    pivot = pivot[periodos]
    
    # Calcular promedio ponderado por municipio
    promedios_municipios = {}
    
    for municipio in df['NOM_MUN'].unique():
        df_muni = df[df['NOM_MUN'] == municipio]
        total_registros = df_muni['TOTAL_REGISTROS'].sum()
        total_inseguros = df_muni['TOTAL_INSEGUROS'].sum()
        
        if total_registros > 0:
            promedio = round((total_inseguros / total_registros) * 100, 2)
            promedios_municipios[municipio] = promedio
        else:
            promedios_municipios[municipio] = None
    
    # Añadir columna de promedio histórico
    pivot['PROMEDIO'] = pd.Series(promedios_municipios)
    
    # Ordenar por promedio descendente
    pivot = pivot.sort_values('PROMEDIO', ascending=False)
    
    # Convertir a HTML con estilos
    html = """<div class='table-container'>\n<table class='data-table pivot-table'>\n"""
    html += "<caption>Percepción de Inseguridad por Municipio y Periodo (%)</caption>\n"
    
    # Cabecera
    html += "<thead>\n<tr>\n<th>Municipio</th>\n"
    for periodo in periodos:
        html += f"<th>{periodo}</th>\n"
    html += "<th>PROMEDIO</th>\n</tr>\n</thead>\n"
    
    # Cuerpo de la tabla
    html += "<tbody>\n"
    
    for municipio, row in pivot.iterrows():
        html += "<tr>\n"
        html += f"<td>{municipio}</td>\n"
        
        # Cada periodo
        for periodo in periodos:
            valor = row.get(periodo)
            
            if pd.isna(valor):
                html += "<td class='sin-datos'>-</td>\n"
            else:
                # Determinar clase según nivel de inseguridad
                if valor < 30:
                    clase = "seguro"
                elif valor < 60:
                    clase = "precaucion"
                else:
                    clase = "inseguro"
                html += f"<td class='{clase}'>{valor:.1f}</td>\n"
        
        # Promedio
        promedio = row.get('PROMEDIO')
        if pd.isna(promedio):
            html += "<td class='sin-datos'>-</td>\n"
        else:
            # Determinar clase según nivel
            if promedio < 30:
                clase = "seguro"
            elif promedio < 60:
                clase = "precaucion"
            else:
                clase = "inseguro"
            html += f"<td class='{clase}'>{promedio:.1f}</td>\n"
        
        html += "</tr>\n"
    
    html += "</tbody>\n</table>\n</div>"
    
    return html


def generar_html_completo(
    df: pd.DataFrame, 
    df_estatal: pd.DataFrame,
    promedio_estatal: float,
    imagen_estatal: str,
    imagenes_municipios: Dict[str, Tuple[str, float, int]],
    tabla_html: str,
    periodos: List[str],
    logger: logging.Logger
) -> str:
    """
    Genera el reporte HTML completo.
    
    Args:
        df: DataFrame con datos filtrados por estado
        df_estatal: DataFrame con promedios estatales por periodo
        promedio_estatal: Valor del promedio histórico estatal
        imagen_estatal: Imagen en base64 del gráfico estatal
        imagenes_municipios: Dict con imágenes en base64 de los municipios
        tabla_html: HTML de la tabla pivote
        periodos: Lista de periodos procesados
        logger: Logger para registrar eventos
        
    Returns:
        Contenido HTML completo del reporte
    """
    # Fecha actual para metadata
    fecha_generacion = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    
    # Determinar el rango de periodos cubierto
    rango_periodos = f"{periodos[0]} a {periodos[-1]}" if len(periodos) > 1 else periodos[0]
    
    # Obtener municipios ordenados por promedio histórico
    municipios_ordenados = sorted(
        imagenes_municipios.keys(),
        key=lambda x: imagenes_municipios[x][1],
        reverse=True
    )
    
    # Índice con anclas
    indice_html = "<ul>\n"
    indice_html += "<li><a href='#promedio-estatal'>Promedio Estatal</a></li>\n"
    indice_html += "<li><a href='#analisis-municipios'>Análisis por Municipio</a>\n<ul>\n"
    
    for municipio in municipios_ordenados:
        anchor = municipio.lower().replace(" ", "-")
        indice_html += f"<li><a href='#{anchor}'>{municipio}</a></li>\n"
    
    indice_html += "</ul></li>\n"
    indice_html += "<li><a href='#tabla-completa'>Tabla Completa</a></li>\n"
    indice_html += "<li><a href='#notas-metodologicas'>Notas Metodológicas</a></li>\n"
    indice_html += "</ul>\n"
    
    # Contenido de secciones
    seccion_estatal = f"""<section id="promedio-estatal">\n
    <h2>Promedio Estatal</h2>\n
    <p>El promedio estatal de percepción de inseguridad en Yucatán para el periodo 
    analizado ({rango_periodos}) es de <strong>{promedio_estatal}%</strong>.</p>\n
    <div class="img-container">\n
        <img src="data:image/png;base64,{imagen_estatal}" 
            alt="Gráfico de evolución estatal" class="img-responsive">\n
    </div>\n
</section>\n"""
    
    # Sección de municipios
    seccion_municipios = "<section id=\"analisis-municipios\">\n\n    <h2>Análisis por Municipio</h2>\n\n"
    
    # Municipios con datos incompletos
    municipios_incompletos = [m for m, datos in imagenes_municipios.items() 
                           if datos[2] < len(periodos)]
    
    if municipios_incompletos:
        seccion_municipios += "<div class='nota-informativa'>\n"
        seccion_municipios += "<p><strong>Nota:</strong> Los siguientes municipios tienen datos "
        seccion_municipios += "incompletos (no están disponibles para todos los periodos):</p>\n"
        seccion_municipios += "<ul>\n"
        
        for municipio in sorted(municipios_incompletos):
            periodos_disponibles = imagenes_municipios[municipio][2]
            seccion_municipios += f"<li>{municipio} ({periodos_disponibles} de {len(periodos)} periodos)</li>\n"
        
        seccion_municipios += "</ul>\n</div>\n"
    
    # Grid de gráficos de municipios
    seccion_municipios += "<div class=\"municipios-grid\">\n"
    
    for municipio in municipios_ordenados:
        imagen_base64, promedio_muni, num_periodos = imagenes_municipios[municipio]
        anchor = municipio.lower().replace(" ", "-")
        
        seccion_municipios += f"<div id=\"{anchor}\" class=\"municipio-card\">\n"
        seccion_municipios += f"<img src=\"data:image/png;base64,{imagen_base64}\" "
        seccion_municipios += f"alt=\"Gráfico de {municipio}\" class=\"img-responsive\">\n"
        seccion_municipios += "</div>\n"
    
    seccion_municipios += "</div>\n</section>\n"
    
    # Sección de tabla completa
    seccion_tabla = f"""<section id="tabla-completa">\n
    <h2>Tabla Completa por Municipio y Periodo</h2>\n
    <p>La siguiente tabla muestra la percepción de inseguridad para cada municipio 
    y periodo analizado. Los valores representan el porcentaje de personas que se 
    sienten inseguras.</p>\n
    {tabla_html}\n
</section>\n"""
    
    # Notas metodológicas
    seccion_notas = """<section id="notas-metodologicas">

    <h2>Notas Metodológicas</h2>

    <p>Este reporte se basa en datos de la Encuesta Nacional de Seguridad Pública Urbana (ENSU) 
    del INEGI, con las siguientes consideraciones metodológicas:</p>

    <ul>
        <li>La pregunta analizada es "BP1_1: ¿Se siente seguro(a) en su municipio?", 
        donde las respuestas se codifican como: 1=Seguro, 2=Inseguro, 9=No responde.</li>
        
        <li>Los porcentajes se calculan sobre el total de respuestas válidas para cada 
        municipio y periodo.</li>
        
        
        <li>Los promedios estatales están ponderados por el número de registros 
        disponibles en cada municipio.</li>
        
        <li>Las series temporales muestran la evolución trimestral de los datos.</li>
        
        <li>Colores: <span class="seguro-text">Verde</span> indica nivel bajo de percepción de inseguridad (&lt;30%), 
        <span class="precaucion-text">Amarillo</span> indica nivel medio (30-60%), 
        <span class="inseguro-text">Rojo</span> indica nivel alto (&gt;60%).</li>
    </ul>
    
    <p>Fuente de datos: Instituto Nacional de Estadística y Geografía (INEGI) - 
    Encuesta Nacional de Seguridad Pública Urbana (ENSU).</p>

</section>
"""
    
    # Generar HTML completo con estilos CSS
    html = f"""<!DOCTYPE html>

<html lang="es">

<head>

    <meta charset="UTF-8">

    <meta name="viewport" content="width=device-width, initial-scale=1.0">

    <title>Reporte de Percepción de Seguridad en Yucatán</title>

    <style>
        /* Estilos generales */
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
            line-height: 1.6;
            color: #333;
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
        }
        
        h1, h2, h3 {
            color: #2c3e50;
        }
        
        h1 {
            font-size: 2em;
            margin-bottom: 0.5em;
        }
        
        h2 {
            font-size: 1.6em;
            border-bottom: 1px solid #eee;
            padding-bottom: 0.3em;
            margin-top: 1.5em;
        }
        
        /* Header */
        header {
            margin-bottom: 2em;
            text-align: center;
        }
        
        header p {
            color: #7f8c8d;
            margin: 0.2em 0;
        }
        
        /* Navegación */
        nav {
            margin: 2em 0;
            padding: 1em;
            background-color: #f8f9fa;
            border-radius: 5px;
        }
        
        nav h2 {
            font-size: 1.2em;
            margin: 0 0 0.5em 0;
        }
        
        nav ul {
            margin: 0;
            padding-left: 1.5em;
        }
        
        nav li {
            margin-bottom: 0.3em;
        }
        
        /* Imágenes */
        .img-container {
            text-align: center;
            margin: 2em 0;
        }
        
        .img-responsive {
            max-width: 100%;
            height: auto;
        }
        
        /* Grid de municipios */
        .municipios-grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(350px, 1fr));
            gap: 20px;
            margin: 2em 0;
        }
        
        .municipio-card {
            background-color: #f8f9fa;
            border-radius: 5px;
            padding: 15px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        }
        
        /* Tablas */
        .table-container {
            overflow-x: auto;
            margin: 2em 0;
        }
        
        .data-table {
            width: 100%;
            border-collapse: collapse;
            font-size: 0.9em;
        }
        
        .data-table caption {
            font-weight: bold;
            margin-bottom: 0.5em;
        }
        
        .data-table th, .data-table td {
            padding: 8px;
            text-align: center;
            border: 1px solid #ddd;
        }
        
        .data-table th {
            background-color: #f2f2f2;
            position: sticky;
            top: 0;
        }
        
        .data-table tr:nth-child(even) {
            background-color: #f9f9f9;
        }
        
        .data-table tr:hover {
            background-color: #f1f1f1;
        }
        
        /* Colores para niveles de seguridad */
        .seguro {
            background-color: rgba(76, 175, 80, 0.2);
        }
        
        .precaucion {
            background-color: rgba(255, 193, 7, 0.2);
        }
        
        .inseguro {
            background-color: rgba(244, 67, 54, 0.2);
        }
        
        .seguro-text {
            color: #2e7d32;
        }
        
        .precaucion-text {
            color: #ff8f00;
        }
        
        .inseguro-text {
            color: #c62828;
        }
        
        .sin-datos {
            background-color: #f5f5f5;
            color: #9e9e9e;
        }
        
        /* Notas y alertas */
        .nota-informativa {
            background-color: #e3f2fd;
            border-left: 4px solid #2196f3;
            padding: 12px;
            margin: 1em 0;
            border-radius: 0 5px 5px 0;
        }
        
        /* Botón volver arriba */
        .back-to-top {
            position: fixed;
            bottom: 20px;
            right: 20px;
            background-color: #2c3e50;
            color: white;
            width: 40px;
            height: 40px;
            text-align: center;
            line-height: 40px;
            border-radius: 50%;
            text-decoration: none;
            opacity: 0.7;
            z-index: 1000;
        }
        
        .back-to-top:hover {
            opacity: 1;
        }
        
        /* Responsive */
        @media (max-width: 768px) {
            body {
                padding: 10px;
            }
            
            .municipios-grid {
                grid-template-columns: 1fr;
            }
        }
    </style>
</head>
<body>
    <header>
        <h1>Reporte de Percepción de Seguridad en Yucatán</h1>
        <p>Periodo analizado: {rango_periodos}</p>
        <p>Fecha de generación: {fecha_generacion}</p>
        <p>Fuente: INEGI - Encuesta Nacional de Seguridad Pública Urbana (ENSU)</p>
    </header>
    <nav>
        <h2>Índice</h2>
        {indice_html}
    </nav>
    {seccion_estatal}
    {seccion_municipios}
    {seccion_tabla}
    {seccion_notas}
    <a href="#" class="back-to-top" title="Volver arriba">↑</a>
</body>
</html>
"""
    
    # Registrar en el log
    logger.info(f"Reporte HTML generado con {len(municipios_ordenados)} municipios")
    
    return html


def main():
    """Función principal del script."""
    parser = argparse.ArgumentParser(
        description="Generador de reportes de evolución de percepción de inseguridad en Yucatán."
    )
    parser.add_argument(
        "--estado", 
        default="YUCATAN",
        help="Estado a filtrar (default: YUCATAN)"
    )
    parser.add_argument(
        "--dir_procesados", 
        default=".\\data\\yucatan-inseguridad",
        help="Directorio con archivos procesados (default: .\\data\\yucatan-inseguridad)"
    )
    parser.add_argument(
        "--dir_reportes", 
        default=".\\data\\yucatan-inseguridad\\reportes",
        help="Directorio para guardar reportes (default: .\\data\\yucatan-inseguridad\\reportes)"
    )
    parser.add_argument(
        "--log", 
        default=".\\data\\yucatan-inseguridad\\logs\\reporte.log",
        help="Archivo de log (default: .\\data\\yucatan-inseguridad\\logs\\reporte.log)"
    )
    
    args = parser.parse_args()
    
    try:
        # Configurar logging
        logger = setup_logging(args.log)
        logger.info(f"Iniciando generación de reporte para el estado: {args.estado}")
        
        # Cargar datos procesados
        df = cargar_datos_procesados(args.dir_procesados, args.estado, logger)
        
        # Calcular promedio estatal
        df_estatal, promedio_estatal = calcular_promedio_estatal(df, logger)
        
        # Identificar periodos y disponibilidad por municipio
        periodos, periodos_por_municipio, conteo_periodos = identificar_periodos_municipios(df)
        
        # Detectar municipios con gaps
        municipios_con_gaps = []
        for municipio, periodos_muni in periodos_por_municipio.items():
            if len(periodos_muni) < len(periodos):
                periodos_faltantes = set(periodos) - periodos_muni
                municipios_con_gaps.append(
                    (municipio, len(periodos) - len(periodos_muni), ", ".join(sorted(periodos_faltantes)))
                )
        
        if municipios_con_gaps:
            # Limitar a los 5 municipios con más gaps para el log
            municipios_con_gaps_top = sorted(municipios_con_gaps, key=lambda x: x[1], reverse=True)[:5]
            logger.info(f"Municipios con datos incompletos (top 5):")
            for municipio, num_gaps, periodos_faltantes in municipios_con_gaps_top:
                logger.info(f"  - {municipio}: {num_gaps} periodos faltantes ({periodos_faltantes})")
        
        # Generar gráfico estatal
        imagen_estatal = generar_grafico_estatal(df_estatal, promedio_estatal)
        
        # Generar gráficos por municipio
        logger.info("Generando gráficos municipales...")
        imagenes_municipios = {}
        
        for municipio in df["NOM_MUN"].unique():
            imagen, promedio, num_periodos = generar_grafico_municipio(
                df, municipio, periodos, periodos_por_municipio[municipio]
            )
            imagenes_municipios[municipio] = (imagen, promedio, num_periodos)
        
        # Generar tabla pivote
        tabla_html = generar_tabla_pivote(df, periodos)
        
        # Generar HTML completo
        html = generar_html_completo(
            df=df,
            df_estatal=df_estatal,
            promedio_estatal=promedio_estatal,
            imagen_estatal=imagen_estatal,
            imagenes_municipios=imagenes_municipios,
            tabla_html=tabla_html,
            periodos=periodos,
            logger=logger
        )
        
        # Crear directorio de reportes si no existe
        os.makedirs(args.dir_reportes, exist_ok=True)
        
        # Guardar reporte HTML
        reporte_path = os.path.join(args.dir_reportes, "reporte_yucatan_evolucion.html")
        with open(reporte_path, "w", encoding="utf-8") as f:
            f.write(html)
        
        logger.info(f"Reporte HTML guardado en: {reporte_path}")
        
        # Guardar datos tabulados
        datos_path = os.path.join(args.dir_reportes, "datos_tabulados_yucatan.csv")
        df.to_csv(datos_path, index=False)
        logger.info(f"Datos tabulados guardados en: {datos_path}")
        
        print(f"\nReporte generado exitosamente en: {reporte_path}")
        print(f"Datos tabulados guardados en: {datos_path}")
        
    except Exception as e:
        if 'logger' in locals():
            logger.exception(f"Error durante la generación del reporte: {str(e)}")
        else:
            print(f"Error durante la generación del reporte: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
