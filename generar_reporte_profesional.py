#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Generador profesional de reportes para análisis de percepción de seguridad ENSU.
Crea reportes HTML interactivos con múltiples visualizaciones y comparativas.

Autor: Cascade AI
Fecha: 2025-09-10
"""

import os
import sys
import glob
import json
import logging
import argparse
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Set, Optional, Union, Any

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')  # No usar interfaz gráfica
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.colors import LinearSegmentedColormap
import matplotlib.ticker as ticker
import base64
from io import BytesIO


def setup_logging(log_file: str) -> logging.Logger:
    """Configura el sistema de logging."""
    # Crear directorio para logs si no existe
    log_dir = os.path.dirname(log_file)
    os.makedirs(log_dir, exist_ok=True)
    
    # Configurar logger
    logger = logging.getLogger("reporte_profesional")
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


def cargar_resumen_procesamiento(dir_logs: str, logger: logging.Logger) -> Dict[str, Any]:
    """
    Carga el resumen de procesamiento de los archivos.
    """
    ruta_resumen = os.path.join(dir_logs, "resumen_procesamiento.json")
    try:
        with open(ruta_resumen, 'r', encoding='utf-8') as f:
            resumen = json.load(f)
        
        logger.info(f"Resumen de procesamiento cargado: {len(resumen['resultados_detallados'])} archivos")
        return resumen
    except Exception as e:
        logger.warning(f"No se pudo cargar el resumen de procesamiento: {str(e)}")
        return {}


def calcular_promedio_estatal(df: pd.DataFrame, logger: logging.Logger) -> Tuple[pd.DataFrame, float]:
    """
    Calcula el promedio estatal ponderado por periodo.
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
        total_seguros = df_periodo["TOTAL_SEGUROS"].sum()
        total_no_responde = df_periodo["TOTAL_NO_RESPONDE"].sum()
        
        # Calcular porcentajes ponderados
        if total_registros > 0:
            pct_inseguros = (total_inseguros / total_registros) * 100
            pct_seguros = (total_seguros / total_registros) * 100
            pct_no_responde = (total_no_responde / total_registros) * 100
        else:
            pct_inseguros = pct_seguros = pct_no_responde = 0
        
        promedios_estatales.append({
            "PERIODO": periodo,
            "AÑO": df_periodo["AÑO"].iloc[0],
            "TRIMESTRE": df_periodo["TRIMESTRE"].iloc[0],
            "TOTAL_REGISTROS_ESTATAL": total_registros,
            "TOTAL_INSEGUROS_ESTATAL": total_inseguros,
            "TOTAL_SEGUROS_ESTATAL": total_seguros,
            "TOTAL_NO_RESPONDE_ESTATAL": total_no_responde,
            "PCT_INSEGUROS_ESTATAL": round(pct_inseguros, 2),
            "PCT_SEGUROS_ESTATAL": round(pct_seguros, 2),
            "PCT_NO_RESPONDE_ESTATAL": round(pct_no_responde, 2)
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


def generar_grafico_tendencia_estatal(df_estatal: pd.DataFrame, promedio_historico: float) -> str:
    """
    Genera un gráfico de tendencia estatal avanzado con bandas de confianza y áreas sombreadas.
    """
    # Configuración de estilo profesional
    plt.style.use('seaborn-v0_8-whitegrid')
    
    # Crear figura de alta resolución
    fig, ax = plt.subplots(figsize=(12, 6), dpi=100)
    
    # Obtener datos
    periodos = df_estatal["PERIODO"].tolist()
    valores = df_estatal["PCT_INSEGUROS_ESTATAL"].tolist()
    valores_seguros = df_estatal["PCT_SEGUROS_ESTATAL"].tolist()
    
    # Crear paleta de colores personalizada
    colores_inseguridad = ['#2ecc71', '#f1c40f', '#e74c3c']
    cmap_inseguridad = LinearSegmentedColormap.from_list('seguridad', colores_inseguridad)
    
    # Definir colores según niveles
    colores = [cmap_inseguridad(v/100) for v in valores]
    
    # Crear gráfico de línea principal
    ax.plot(periodos, valores, marker='o', linestyle='-', color='#3498db', linewidth=3, 
            zorder=5, label='% Percepción de inseguridad')
    
    # Añadir línea de seguridad
    ax.plot(periodos, valores_seguros, marker='s', linestyle='--', color='#2ecc71', linewidth=2,
            alpha=0.7, zorder=4, label='% Percepción de seguridad')
    
    # Añadir puntos con color según nivel
    for i, (periodo, valor) in enumerate(zip(periodos, valores)):
        ax.scatter(periodo, valor, color=colores[i], s=100, zorder=10, edgecolor='white', linewidth=1.5)
    
    # Añadir línea de promedio histórico
    ax.axhline(y=promedio_historico, color='#e74c3c', linestyle='--', alpha=0.7, 
              linewidth=2, label=f'Promedio histórico: {promedio_historico}%')
    
    # Añadir área sombreada para niveles de riesgo
    ax.axhspan(0, 30, alpha=0.1, color='#2ecc71', label='Bajo riesgo')
    ax.axhspan(30, 60, alpha=0.1, color='#f1c40f', label='Riesgo medio')
    ax.axhspan(60, 100, alpha=0.1, color='#e74c3c', label='Alto riesgo')
    
    # Añadir etiquetas de datos
    for i, (periodo, valor) in enumerate(zip(periodos, valores)):
        ax.annotate(f'{valor:.1f}%', 
                   (periodo, valor), 
                   xytext=(0, 10),
                   textcoords='offset points',
                   ha='center',
                   fontsize=9,
                   fontweight='bold',
                   bbox=dict(boxstyle="round,pad=0.3", fc="white", ec="gray", alpha=0.8))
    
    # Añadir etiquetas y título
    ax.set_title("Evolución de la Percepción de Inseguridad en Yucatán", 
                fontsize=16, fontweight='bold', pad=20)
    ax.set_xlabel("Periodo", fontsize=12, fontweight='bold')
    ax.set_ylabel("Porcentaje (%)", fontsize=12, fontweight='bold')
    
    # Añadir grid
    ax.grid(True, alpha=0.3, linestyle='--')
    
    # Añadir leyenda
    ax.legend(loc='upper center', bbox_to_anchor=(0.5, -0.15), ncol=3, frameon=True, 
             fancybox=True, shadow=True, fontsize=10)
    
    # Rotar etiquetas del eje X
    plt.xticks(rotation=45, ha='right')
    
    # Añadir límites y márgenes
    ax.set_ylim(0, 100)
    
    # Añadir título secundario
    plt.figtext(0.5, 0.01, "Fuente: INEGI - Encuesta Nacional de Seguridad Pública Urbana (ENSU)", 
                ha='center', fontsize=8, style='italic')
    
    # Ajustar diseño
    plt.tight_layout()
    
    # Guardar imagen en memoria
    buffer = BytesIO()
    plt.savefig(buffer, format='png', dpi=100, bbox_inches='tight')
    plt.close()
    
    # Codificar imagen en base64
    buffer.seek(0)
    imagen_base64 = base64.b64encode(buffer.read()).decode('utf-8')
    
    return imagen_base64


def generar_mapa_calor_municipios(df: pd.DataFrame) -> str:
    """
    Genera un mapa de calor de los municipios con mayor percepción de inseguridad.
    """
    # Seleccionar los municipios con más datos
    top_municipios = df.groupby('NOM_MUN')['TOTAL_REGISTROS'].sum().nlargest(10).index.tolist()
    
    # Filtrar DataFrame para los municipios top y crear pivot
    df_top = df[df['NOM_MUN'].isin(top_municipios)]
    pivot = pd.pivot_table(
        df_top,
        values='PCT_INSEGUROS',
        index=['NOM_MUN'],
        columns=['PERIODO'],
        aggfunc='first'
    )
    
    # Ordenar filas por promedio de inseguridad
    promedios = pivot.mean(axis=1).sort_values(ascending=False)
    pivot = pivot.reindex(promedios.index)
    
    # Configurar el gráfico
    plt.style.use('seaborn-v0_8-whitegrid')
    fig, ax = plt.subplots(figsize=(12, 8), dpi=100)
    
    # Paleta de colores
    cmap = LinearSegmentedColormap.from_list('seguridad', ['#2ecc71', '#f1c40f', '#e74c3c'])
    
    # Crear mapa de calor
    im = ax.imshow(pivot.values, cmap=cmap, aspect='auto', vmin=0, vmax=100)
    
    # Configurar ejes
    ax.set_xticks(np.arange(len(pivot.columns)))
    ax.set_yticks(np.arange(len(pivot.index)))
    ax.set_xticklabels(pivot.columns, rotation=45, ha="right")
    ax.set_yticklabels(pivot.index)
    
    # Añadir valores a las celdas
    for i in range(len(pivot.index)):
        for j in range(len(pivot.columns)):
            valor = pivot.iloc[i, j]
            if not pd.isna(valor):
                text_color = 'white' if valor > 40 else 'black'
                ax.text(j, i, f"{valor:.1f}%", ha="center", va="center", 
                       color=text_color, fontweight='bold')
    
    # Añadir colorbar
    cbar = fig.colorbar(im, ax=ax, orientation='vertical', shrink=0.8)
    cbar.set_label('% Percepción de inseguridad', rotation=270, labelpad=20)
    
    # Añadir título
    ax.set_title("Mapa de Calor de Percepción de Inseguridad por Municipio", 
                fontsize=16, fontweight='bold', pad=20)
    
    # Ajustar diseño
    plt.tight_layout()
    
    # Guardar imagen en memoria
    buffer = BytesIO()
    plt.savefig(buffer, format='png', dpi=100, bbox_inches='tight')
    plt.close()
    
    # Codificar imagen en base64
    buffer.seek(0)
    imagen_base64 = base64.b64encode(buffer.read()).decode('utf-8')
    
    return imagen_base64


def generar_comparativa_municipios(df: pd.DataFrame, ultimo_periodo: str) -> str:
    """
    Genera un gráfico de barras horizontal comparando los municipios en el último periodo.
    """
    # Filtrar por último periodo
    df_ultimo = df[df['PERIODO'] == ultimo_periodo].copy()
    
    # Ordenar por porcentaje de inseguridad
    df_ultimo = df_ultimo.sort_values('PCT_INSEGUROS', ascending=True)
    
    # Tomar hasta 15 municipios para no saturar el gráfico
    if len(df_ultimo) > 15:
        df_ultimo = df_ultimo.tail(15)
    
    # Configurar el gráfico
    plt.style.use('seaborn-v0_8-whitegrid')
    fig, ax = plt.subplots(figsize=(10, 8), dpi=100)
    
    # Obtener datos
    municipios = df_ultimo['NOM_MUN'].tolist()
    valores = df_ultimo['PCT_INSEGUROS'].tolist()
    
    # Crear paleta de colores
    colores = ['#2ecc71' if v < 30 else '#f1c40f' if v < 60 else '#e74c3c' for v in valores]
    
    # Crear gráfico de barras horizontal
    bars = ax.barh(municipios, valores, color=colores, edgecolor='grey', linewidth=0.5)
    
    # Añadir valores a las barras
    for i, bar in enumerate(bars):
        width = bar.get_width()
        label_x_pos = width + 1  # Posicionar etiqueta fuera de la barra
        ax.text(label_x_pos, bar.get_y() + bar.get_height()/2, f'{valores[i]:.1f}%',
               va='center', fontsize=9, fontweight='bold')
    
    # Añadir título y etiquetas
    ax.set_title(f"Percepción de Inseguridad por Municipio - {ultimo_periodo}", 
                fontsize=16, fontweight='bold')
    ax.set_xlabel('Porcentaje de percepción de inseguridad (%)', fontsize=12)
    
    # Añadir grid vertical
    ax.grid(axis='x', alpha=0.3, linestyle='--')
    
    # Añadir límites
    ax.set_xlim(0, max(valores) + 10)
    
    # Ajustar diseño
    plt.tight_layout()
    
    # Guardar imagen en memoria
    buffer = BytesIO()
    plt.savefig(buffer, format='png', dpi=100, bbox_inches='tight')
    plt.close()
    
    # Codificar imagen en base64
    buffer.seek(0)
    imagen_base64 = base64.b64encode(buffer.read()).decode('utf-8')
    
    return imagen_base64
