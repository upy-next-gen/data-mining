#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Generador de reportes de evolución de percepción de inseguridad en Yucatán.

Este script genera un reporte HTML con visualizaciones que muestran la evolución
temporal del porcentaje de percepción de inseguridad en el estado de Yucatán.

Author: Cascade AI
Date: 2025-09-09
"""
import os
import sys
import re
import glob
import logging
import argparse
import base64
from io import BytesIO
from datetime import datetime
from typing import Dict, List, Tuple, Set, Optional, Union, Any

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')  # No usar interfaz gráfica
import matplotlib.pyplot as plt


# Configuración de logging
def setup_logging(reportes_dir: str, timestamped: bool = False) -> logging.Logger:
    """
    Configura el sistema de logging para el análisis.
    
    Args:
        reportes_dir: Directorio donde guardar los logs
        timestamped: Si se debe incluir timestamp en el nombre del archivo
    
    Returns:
        Logger configurado
    """
    os.makedirs(reportes_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = f"log_analisis_yucatan_{timestamp}.log" if timestamped else "log_analisis_yucatan.log"
    log_path = os.path.join(reportes_dir, log_file)
    
    logger = logging.getLogger("reporte_evolucion")
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


def cargar_datos_procesados(directorio_datos: str, estado: str, logger: logging.Logger) -> pd.DataFrame:
    """
    Carga todos los archivos procesados del directorio especificado y los combina.
    
    Args:
        directorio_datos: Ruta al directorio con los datos procesados
        estado: Nombre del estado a filtrar (normalizado)
        logger: Logger para registro de eventos
        
    Returns:
        DataFrame combinado y filtrado por el estado especificado
    """
    # Buscar archivos procesados
    patron_archivos = os.path.join(directorio_datos, "procesado_*.csv")
    archivos = glob.glob(patron_archivos)
    
    if not archivos:
        logger.error(f"No se encontraron archivos procesados en '{directorio_datos}'")
        sys.exit(1)
    
    logger.info(f"Se encontraron {len(archivos)} archivos de datos procesados:")
    for archivo in archivos:
        logger.info(f"  - {os.path.basename(archivo)}")
    
    # Cargar y combinar archivos
    dataframes = []
    
    for archivo in archivos:
        try:
            df = pd.read_csv(archivo)
            # Extraer año y trimestre del nombre del archivo para verificación
            match = re.search(r'procesado_(\d{4})_(Q\d)_', os.path.basename(archivo))
            if match:
                año_archivo = int(match.group(1))
                trimestre_archivo = match.group(2)
                
                # Verificar que los datos internos coincidan con el nombre
                if df["AÑO"].iloc[0] != año_archivo or df["TRIMESTRE"].iloc[0] != trimestre_archivo:
                    logger.warning(
                        f"Inconsistencia en {os.path.basename(archivo)}: "
                        f"Nombre indica {año_archivo}-{trimestre_archivo}, "
                        f"pero contiene {df['AÑO'].iloc[0]}-{df['TRIMESTRE'].iloc[0]}"
                    )
            
            dataframes.append(df)
        except Exception as e:
            logger.error(f"Error al cargar {archivo}: {str(e)}")
            continue
    
    if not dataframes:
        logger.error("No se pudo cargar ningún archivo de datos")
        sys.exit(1)
    
    # Combinar todos los DataFrames
    df_combinado = pd.concat(dataframes, ignore_index=True)
    
    # Filtrar por estado (normalizado)
    df_filtrado = df_combinado[df_combinado["NOM_ENT"] == estado.upper()]
    
    if len(df_filtrado) == 0:
        logger.error(f"No hay datos para el estado '{estado}'")
        sys.exit(1)
    
    logger.info(f"Se cargaron datos de {len(df_filtrado)} municipios de {estado.upper()}")
    
    # Detectar y corregir trimestres duplicados
    duplicados = df_filtrado.duplicated(subset=["NOM_MUN", "AÑO", "TRIMESTRE"], keep=False)
    
    if duplicados.any():
        municipios_duplicados = df_filtrado[duplicados]["NOM_MUN"].unique()
        logger.warning(
            f"Se detectaron trimestres duplicados para {len(municipios_duplicados)} municipios. "
            f"Se mantiene la última versión de cada combinación municipio-año-trimestre."
        )
        
        # Mantener la última aparición de cada combinación (asumiendo que es la más reciente)
        df_filtrado = df_filtrado.drop_duplicates(
            subset=["NOM_MUN", "AÑO", "TRIMESTRE"], 
            keep="last"
        )
    
    return df_filtrado


def calcular_promedio_estatal(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """
    Calcula el promedio estatal ponderado de percepción de inseguridad por periodo.
    
    Args:
        df: DataFrame con datos filtrados por estado
        logger: Logger para registro de eventos
        
    Returns:
        DataFrame con promedios estatales por periodo
    """
    # Crear clave de periodo para facilitar el agrupamiento
    df['PERIODO'] = df['AÑO'].astype(str) + '-' + df['TRIMESTRE']
    
    # Agrupar por periodo y calcular sumas
    df_agrupado = df.groupby('PERIODO').agg({
        'TOTAL_INSEGUROS': 'sum',
        'TOTAL_REGISTROS': 'sum'
    }).reset_index()
    
    # Calcular porcentaje estatal ponderado
    df_agrupado['PCT_INSEGUROS_ESTATAL'] = round(
        (df_agrupado['TOTAL_INSEGUROS'] / df_agrupado['TOTAL_REGISTROS']) * 100,
        2
    )
    
    # Ordenar por año y trimestre
    def ordenar_periodo(periodo):
        año, trimestre = periodo.split('-')
        num_trimestre = int(trimestre[1])
        return int(año), num_trimestre
    
    df_agrupado['orden'] = df_agrupado['PERIODO'].apply(ordenar_periodo)
    df_agrupado = df_agrupado.sort_values('orden')
    df_agrupado.drop('orden', axis=1, inplace=True)
    
    promedio_historico = round(
        (df_agrupado['TOTAL_INSEGUROS'].sum() / df_agrupado['TOTAL_REGISTROS'].sum()) * 100,
        2
    )
    
    logger.info(f"Promedio estatal histórico de percepción de inseguridad: {promedio_historico}%")
    logger.info(f"Periodos procesados: {', '.join(df_agrupado['PERIODO'])}")
    
    return df_agrupado, promedio_historico


def identificar_periodos_municipios(df: pd.DataFrame) -> Tuple[List[str], Dict[str, Set[str]], Dict[str, int]]:
    """
    Identifica todos los periodos y los periodos disponibles por municipio.
    
    Args:
        df: DataFrame con datos filtrados por estado
        
    Returns:
        Tuple con (lista_periodos, dict_periodos_por_municipio, dict_conteo_periodos)
    """
    # Crear clave de periodo para facilitar el agrupamiento
    if 'PERIODO' not in df.columns:
        df['PERIODO'] = df['AÑO'].astype(str) + '-' + df['TRIMESTRE']
    
    # Obtener lista única de periodos ordenados
    def ordenar_periodo(periodo):
        año, trimestre = periodo.split('-')
        num_trimestre = int(trimestre[1])
        return int(año), num_trimestre
    
    periodos = sorted(df['PERIODO'].unique(), key=ordenar_periodo)
    
    # Obtener periodos por municipio
    periodos_por_municipio = {}
    conteo_periodos = {}
    
    for municipio in df['NOM_MUN'].unique():
        periodos_muni = set(df[df['NOM_MUN'] == municipio]['PERIODO'])
        periodos_por_municipio[municipio] = periodos_muni
        conteo_periodos[municipio] = len(periodos_muni)
    
    return periodos, periodos_por_municipio, conteo_periodos


def generar_grafico_estatal(df_estatal: pd.DataFrame, promedio_historico: float) -> str:
    """
    Genera un gráfico con la evolución del promedio estatal de percepción de inseguridad.
    
    Args:
        df_estatal: DataFrame con promedios estatales por periodo
        promedio_historico: Promedio histórico ponderado estatal
        
    Returns:
        String con imagen codificada en base64
    """
    # Determinar color según promedio histórico
    if promedio_historico < 30:
        color = '#2ca02c'  # Verde
    elif promedio_historico < 60:
        color = '#ff7f0e'  # Naranja
    else:
        color = '#d62728'  # Rojo
    
    # Crear figura
    plt.figure(figsize=(10, 6.25), dpi=80)
    
    # Plotear línea principal
    plt.plot(
        df_estatal['PERIODO'], 
        df_estatal['PCT_INSEGUROS_ESTATAL'],
        marker='o',
        linestyle='-',
        linewidth=2.5,
        color=color,
        label='Percepción de inseguridad'
    )
    
    # Línea horizontal de promedio
    plt.axhline(
        y=promedio_historico,
        linestyle='--',
        color='gray',
        alpha=0.7,
        label=f'Promedio: {promedio_historico}%'
    )
    
    # Añadir grid, títulos y etiquetas
    plt.grid(True, alpha=0.3)
    plt.title(
        'Evolución de la percepción de inseguridad en YUCATÁN',
        fontsize=16,
        pad=15
    )
    plt.xlabel('Periodo', fontsize=12)
    plt.ylabel('Porcentaje (%)', fontsize=12)
    plt.ylim(0, 100)
    
    # Leyenda
    plt.legend(loc='best')
    
    # Mostrar valores en cada punto
    for i, v in enumerate(df_estatal['PCT_INSEGUROS_ESTATAL']):
        plt.annotate(
            f"{v}%",
            (i, v),
            textcoords="offset points",
            xytext=(0, 7),
            ha='center'
        )
    
    # Guardar en formato base64
    buffer = BytesIO()
    plt.tight_layout()
    plt.savefig(buffer, format='png')
    plt.close()
    
    return base64.b64encode(buffer.getvalue()).decode('utf-8')


def generar_grafico_municipio(
    df: pd.DataFrame, 
    municipio: str, 
    periodos: List[str], 
    periodos_municipio: Set[str]
) -> Tuple[str, float, int]:
    """
    Genera un gráfico de evolución para un municipio específico.
    
    Args:
        df: DataFrame con datos filtrados
        municipio: Nombre del municipio
        periodos: Lista de todos los periodos posibles
        periodos_municipio: Set con los periodos disponibles para este municipio
        
    Returns:
        Tuple con (imagen_base64, promedio_municipio, num_periodos)
    """
    # Filtrar datos del municipio
    df_muni = df[df['NOM_MUN'] == municipio].copy()
    
    # Crear series completa de periodos con potenciales huecos
    series = []
    for periodo in periodos:
        if periodo in periodos_municipio:
            row = df_muni[df_muni['PERIODO'] == periodo].iloc[0]
            series.append((periodo, row['PCT_INSEGUROS']))
        else:
            series.append((periodo, np.nan))  # Hueco en la serie
    
    # Crear DataFrame para plotear
    df_plot = pd.DataFrame(series, columns=['PERIODO', 'PCT_INSEGUROS'])
    
    # Calcular promedio histórico del municipio (solo con datos disponibles)
    datos_validos = df_muni[df_muni['PERIODO'].isin(periodos_municipio)]
    total_inseguros = datos_validos['TOTAL_INSEGUROS'].sum()
    total_registros = datos_validos['TOTAL_REGISTROS'].sum()
    promedio_historico = round((total_inseguros / total_registros) * 100, 2) if total_registros > 0 else 0
    
    # Determinar color según promedio histórico
    if promedio_historico < 30:
        color = '#2ca02c'  # Verde
    elif promedio_historico < 60:
        color = '#ff7f0e'  # Naranja
    else:
        color = '#d62728'  # Rojo
    
    # Crear figura
    plt.figure(figsize=(8, 4.5), dpi=80)
    
    # Plotear línea principal (con huecos donde hay valores NaN)
    plt.plot(
        df_plot['PERIODO'], 
        df_plot['PCT_INSEGUROS'],
        marker='o',
        linestyle='-',
        linewidth=2,
        color=color
    )
    
    # Línea horizontal de promedio
    plt.axhline(
        y=promedio_historico,
        linestyle='--',
        color='gray',
        alpha=0.7,
        label=f'Promedio: {promedio_historico}%'
    )
    
    # Añadir grid, títulos y etiquetas
    plt.grid(True, alpha=0.3)
    plt.title(
        f'{municipio}',
        fontsize=14,
        pad=10
    )
    plt.xlabel('Periodo', fontsize=10)
    plt.ylabel('Porcentaje (%)', fontsize=10)
    plt.ylim(0, 100)
    
    # Leyenda
    plt.legend(loc='best')
    
    # Mostrar valores en cada punto (excepto NaN)
    for i, v in enumerate(df_plot['PCT_INSEGUROS']):
        if not pd.isna(v):
            plt.annotate(
                f"{v}%",
                (i, v),
                textcoords="offset points",
                xytext=(0, 7),
                ha='center',
                fontsize=8
            )
    
    # Guardar en formato base64
    buffer = BytesIO()
    plt.tight_layout()
    plt.savefig(buffer, format='png')
    plt.close()
    
    return base64.b64encode(buffer.getvalue()).decode('utf-8'), promedio_historico, len(periodos_municipio)


def generar_tabla_pivote(df: pd.DataFrame, periodos: List[str]) -> str:
    """
    Genera una tabla HTML pivote con los porcentajes de inseguridad por municipio y periodo.
    
    Args:
        df: DataFrame con datos filtrados por estado
        periodos: Lista ordenada de periodos
        
    Returns:
        Código HTML de la tabla
    """
    # Preparar datos para tabla pivote
    tabla = pd.pivot_table(
        df,
        values='PCT_INSEGUROS',
        index=['NOM_MUN'],
        columns=['PERIODO'],
        aggfunc='first'
    )
    
    # Calcular columna de promedio
    # Para el promedio, necesitamos volver a los datos originales y calcular ponderadamente
    promedios = {}
    for municipio in df['NOM_MUN'].unique():
        datos_muni = df[df['NOM_MUN'] == municipio]
        total_inseguros = datos_muni['TOTAL_INSEGUROS'].sum()
        total_registros = datos_muni['TOTAL_REGISTROS'].sum()
        promedios[municipio] = round((total_inseguros / total_registros) * 100, 1) if total_registros > 0 else 0
    
    # Añadir columna de promedio
    tabla['PROMEDIO'] = tabla.index.map(promedios)
    
    # Reordenar columnas según los periodos especificados
    columnas_ordenadas = [col for col in periodos if col in tabla.columns] + ['PROMEDIO']
    tabla = tabla[columnas_ordenadas]
    
    # Ordenar filas por promedio descendente
    tabla = tabla.sort_values('PROMEDIO', ascending=False)
    
    # Formatear la tabla para HTML (1 decimal)
    tabla_html = tabla.copy()
    
    # Convertir NaN a cadenas vacías y formatear números
    for col in tabla_html.columns:
        tabla_html[col] = tabla_html[col].apply(
            lambda x: f"{x:.1f}" if pd.notnull(x) else ""
        )
    
    # Generar HTML con estilos
    html = """<div class='table-container'>\n<table class='data-table'>\n"""
    
    # Encabezado
    html += "<thead>\n<tr>\n<th>Municipio</th>\n"
    for col in columnas_ordenadas:
        if col == 'PROMEDIO':
            html += f"<th>{col}</th>\n"
        else:
            html += f"<th>{col}</th>\n"
    html += "</tr>\n</thead>\n"
    
    # Cuerpo
    html += "<tbody>\n"
    for municipio in tabla_html.index:
        html += f"<tr>\n<td>{municipio}</td>\n"
        for col in columnas_ordenadas:
            valor = tabla_html.loc[municipio, col]
            # Añadir clase CSS según valor (solo si no está vacío)
            if valor:  # Si no está vacío
                valor_num = float(valor)
                if valor_num < 30:
                    clase = "seguro"
                elif valor_num < 60:
                    clase = "precaucion"
                else:
                    clase = "inseguro"
                html += f"<td class='{clase}'>{valor}</td>\n"
            else:
                html += "<td></td>\n"  # Celda vacía
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
        logger: Logger para registro de eventos
        
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
    
    # Generar índice con anclas
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
    seccion_municipios = f"""<section id="analisis-municipios">\n<h2>Análisis por Municipio</h2>\n
<p>A continuación se presenta el análisis detallado de la percepción de inseguridad 
   para cada municipio del estado.</p>\n\n"""
    
    # Agregar cada municipio con su gráfico
    for municipio in municipios_ordenados:
        imagen_base64, promedio, num_periodos = imagenes_municipios[municipio]
        anchor = municipio.lower().replace(" ", "-")
        
        # Determinar clase para color de texto según promedio
        if promedio < 30:
            clase_color = "seguro-text"
        elif promedio < 60:
            clase_color = "precaucion-text"
        else:
            clase_color = "inseguro-text"
        
        seccion_municipios += f"""<div id="{anchor}" class="municipio-container">\n
    <h3>{municipio}</h3>\n
    <p>Promedio histórico: <span class="{clase_color}">{promedio}%</span> 
       (basado en {num_periodos} periodo{'s' if num_periodos != 1 else ''})</p>\n
    <div class="img-container">\n
        <img src="data:image/png;base64,{imagen_base64}" 
            alt="Gráfico de evolución {municipio}" class="img-responsive">\n
    </div>\n
</div>\n\n"""
    
    seccion_municipios += "</section>\n"
    
    # Sección tabla
    seccion_tabla = f"""<section id="tabla-completa">\n
<h2>Tabla Completa</h2>\n
<p>Porcentaje de percepción de inseguridad por municipio y periodo.</p>\n
{tabla_html}\n
</section>\n"""
    
    # Sección notas metodológicas
    seccion_notas = """<section id="notas-metodologicas">\n
<h2>Notas Metodológicas</h2>\n
<h3>Fuente de datos</h3>\n<p>Los datos utilizados en este reporte provienen de la Encuesta Nacional de 
   Seguridad Pública Urbana (ENSU) del INEGI.</p>\n
<h3>Indicador de percepción</h3>\n<p>El indicador presentado corresponde al porcentaje de personas que consideran 
   que vivir en su ciudad es inseguro (respuesta "inseguro" a la pregunta sobre 
   percepción de seguridad).</p>\n
<h3>Metodología de cálculo</h3>\n<ul>\n
    <li>Se consideran tres posibles respuestas a la pregunta sobre percepción de 
        seguridad: seguro (1), inseguro (2) o no responde (9).</li>\n
    <li>El porcentaje de percepción de inseguridad se calcula como: 
        (Total de respuestas "inseguro" / Total de respuestas) * 100</li>\n
    <li>El promedio estatal es un promedio ponderado según el número de 
        respuestas por municipio.</li>\n
    <li>Las series temporales pueden presentar discontinuidades (huecos) cuando 
        no hay datos disponibles para ciertos periodos.</li>\n
</ul>\n
<h3>Interpretación de colores</h3>\n
<ul>\n
    <li><span class="seguro-text">Verde</span>: Percepción de inseguridad menor al 30%</li>\n
    <li><span class="precaucion-text">Naranja</span>: Percepción de inseguridad entre 30% y 60%</li>\n
    <li><span class="inseguro-text">Rojo</span>: Percepción de inseguridad superior al 60%</li>\n
</ul>\n
</section>\n"""
    
    # Generar HTML completo con estilos CSS
    html = f"""<!DOCTYPE html>\n
<html lang="es">\n
<head>\n
    <meta charset="UTF-8">\n
    <meta name="viewport" content="width=device-width, initial-scale=1.0">\n
    <title>Reporte de Percepción de Seguridad en Yucatán</title>\n
    <style>\n
        :root {{\n
            --color-primary: #2c3e50;\n
            --color-secondary: #34495e;\n
            --color-accent: #3498db;\n
            --color-text: #333;\n
            --color-light: #f8f9fa;\n
            --color-seguro: #2ca02c;\n
            --color-precaucion: #ff7f0e;\n
            --color-inseguro: #d62728;\n
        }}\n
        * {{\n
            margin: 0;\n
            padding: 0;\n
            box-sizing: border-box;\n
        }}\n
        body {{\n
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, 
                      "Helvetica Neue", Arial, sans-serif;\n
            line-height: 1.6;\n
            color: var(--color-text);\n
            max-width: 1200px;\n
            margin: 0 auto;\n
            padding: 20px;\n
        }}\n
        header {{\n
            background-color: var(--color-primary);\n
            color: white;\n
            padding: 20px;\n
            margin-bottom: 30px;\n
            border-radius: 5px;\n
        }}\n
        header p {{\n
            margin: 5px 0;\n
            opacity: 0.8;\n
            font-size: 0.9em;\n
        }}\n
        nav {{\n
            background-color: var(--color-light);\n
            padding: 15px;\n
            margin-bottom: 30px;\n
            border-radius: 5px;\n
            border-left: 4px solid var(--color-accent);\n
        }}\n
        nav h2 {{\n
            margin-bottom: 10px;\n
            color: var(--color-primary);\n
        }}\n
        ul, ol {{\n
            margin-left: 20px;\n
        }}\n
        nav ul li {{\n
            margin: 5px 0;\n
        }}\n
        a {{\n
            color: var(--color-accent);\n
            text-decoration: none;\n
        }}\n
        a:hover {{\n
            text-decoration: underline;\n
        }}\n
        section {{\n
            margin-bottom: 50px;\n
            padding-bottom: 30px;\n
            border-bottom: 1px solid #eee;\n
        }}\n
        h2 {{\n
            color: var(--color-primary);\n
            margin-bottom: 20px;\n
            padding-bottom: 10px;\n
            border-bottom: 2px solid var(--color-accent);\n
        }}\n
        h3 {{\n
            color: var(--color-secondary);\n
            margin-top: 25px;\n
            margin-bottom: 15px;\n
        }}\n
        p {{\n
            margin-bottom: 20px;\n
        }}\n
        .img-container {{\n
            margin: 20px 0;\n
            text-align: center;\n
        }}\n
        .img-responsive {{\n
            max-width: 100%;\n
            height: auto;\n
            box-shadow: 0 3px 10px rgba(0,0,0,0.1);\n
            border-radius: 5px;\n
        }}\n
        .municipio-container {{\n
            margin-bottom: 40px;\n
            padding: 15px;\n
            background-color: var(--color-light);\n
            border-radius: 5px;\n
        }}\n
        .table-container {{\n
            overflow-x: auto;\n
            margin: 20px 0;\n
        }}\n
        .data-table {{\n
            width: 100%;\n
            border-collapse: collapse;\n
            font-size: 0.9em;\n
        }}\n
        .data-table th {{\n
            background-color: var(--color-secondary);\n
            color: white;\n
            position: sticky;\n
            top: 0;\n
            z-index: 10;\n
        }}\n
        .data-table th, .data-table td {{\n
            padding: 8px 12px;\n
            text-align: center;\n
            border: 1px solid #ddd;\n
        }}\n
        .data-table tr:nth-child(even) {{\n
            background-color: rgba(0,0,0,0.02);\n
        }}\n
        .data-table tr:hover {{\n
            background-color: rgba(0,0,0,0.05);\n
        }}\n
        .data-table td:first-child {{\n
            text-align: left;\n
            font-weight: 500;\n
            position: sticky;\n
            left: 0;\n
            background-color: #f8f9fa;\n
            z-index: 5;\n
        }}\n
        .seguro {{\n
            background-color: rgba(44, 160, 44, 0.1);\n
        }}\n
        .precaucion {{\n
            background-color: rgba(255, 127, 14, 0.1);\n
        }}\n
        .inseguro {{\n
            background-color: rgba(214, 39, 40, 0.1);\n
        }}\n
        .seguro-text {{\n
            color: var(--color-seguro);\n
            font-weight: bold;\n
        }}\n
        .precaucion-text {{\n
            color: var(--color-precaucion);\n
            font-weight: bold;\n
        }}\n
        .inseguro-text {{\n
            color: var(--color-inseguro);\n
            font-weight: bold;\n
        }}\n
        .back-to-top {{\n
            position: fixed;\n
            bottom: 20px;\n
            right: 20px;\n
            background-color: var(--color-primary);\n
            color: white;\n
            width: 40px;\n
            height: 40px;\n
            text-align: center;\n
            line-height: 40px;\n
            border-radius: 50%;\n
            box-shadow: 0 2px 5px rgba(0,0,0,0.2);\n
            opacity: 0.7;\n
            transition: opacity 0.3s;\n
            text-decoration: none;\n
        }}\n
        .back-to-top:hover {{\n
            opacity: 1;\n
        }}\n
        /* Responsive */\n
        @media (max-width: 768px) {{\n
            body {{\n
                padding: 10px;\n
            }}\n
            header, nav, section {{\n
                padding: 10px;\n
            }}\n
            .municipio-container {{\n
                padding: 10px;\n
            }}\n
        }}\n
    </style>\n
</head>\n
<body>\n
    <header>\n
        <h1>Reporte de Percepción de Seguridad en Yucatán</h1>\n
        <p>Periodo analizado: {rango_periodos}</p>\n
        <p>Fecha de generación: {fecha_generacion}</p>\n
        <p>Fuente: INEGI - Encuesta Nacional de Seguridad Pública Urbana (ENSU)</p>\n
    </header>\n
    <nav>\n
        <h2>Índice</h2>\n
        {indice_html}\n
    </nav>\n
    {seccion_estatal}\n
    {seccion_municipios}\n
    {seccion_tabla}\n
    {seccion_notas}\n
    <a href="#" class="back-to-top" title="Volver arriba">↑</a>\n
</body>\n
</html>\n"""
    
    # Registrar en el log
    logger.info(f"Reporte HTML generado con {len(municipios_ordenados)} municipios")
    
    return html


def main():
    """Función principal del script."""
    # Parsear argumentos de línea de comandos
    parser = argparse.ArgumentParser(
        description="Generador de reportes de evolución de percepción de inseguridad en Yucatán."
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
        "--timestamped", 
        action="store_true",
        help="Agregar timestamp a los nombres de archivo de salida"
    )
    
    args = parser.parse_args()
    
    try:
        # Configurar logging
        logger = setup_logging(args.reportes, args.timestamped)
        logger.info(f"Iniciando generación de reporte para el estado: {args.estado}")
        
        # Cargar datos procesados
        df = cargar_datos_procesados(args.salida, args.estado, logger)
        
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
        
        # Guardar el reporte HTML
        reporte_path = os.path.join(args.reportes, "reporte_yucatan_evolucion.html")
        os.makedirs(os.path.dirname(reporte_path), exist_ok=True)
        
        with open(reporte_path, "w", encoding="utf-8") as f:
            f.write(html)
        
        logger.info(f"Reporte HTML guardado en: {reporte_path}")
        
        # Opcionalmente guardar datos tabulados
        datos_tabulados_path = os.path.join(args.reportes, "datos_tabulados_yucatan.csv")
        df.to_csv(datos_tabulados_path, index=False)
        logger.info(f"Datos tabulados guardados en: {datos_tabulados_path}")
        
        logger.info("Proceso de generación de reporte completado exitosamente")
        
    except Exception as e:
        if 'logger' in locals():
            logger.exception(f"Error durante la generación del reporte: {str(e)}")
        else:
            print(f"Error durante la generación del reporte: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
