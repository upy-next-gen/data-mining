
# -*- coding: utf-8 -*-
"""
Generador de Datos para el Dashboard Analítico de Percepción de Inseguridad.

Este script carga los datos procesados, calcula métricas clave (KPIs, 
promedio estatal, tendencias, rankings) y exporta toda la información
a un único archivo JSON que servirá como fuente de datos para el frontend.
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime
from pathlib import Path
import json

# --- CONFIGURACIÓN ---
BASE_DIR = Path(__file__).resolve().parent
DATA_FILE = BASE_DIR / 'dataset_procesado_final.csv'
OUTPUT_DIR = BASE_DIR / 'reporte'
JSON_OUTPUT_FILE = OUTPUT_DIR / 'data.json'
LOG_FILE = OUTPUT_DIR / 'logs_generacion_datos.log'

# --- PARÁMETROS DE ANÁLISIS ---

MOVING_AVERAGE_WINDOW = 4

def configurar_logging():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] [%(levelname)s] - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.FileHandler(LOG_FILE, mode='w', encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    logging.info("Logging para generador de datos configurado.")

def cargar_y_preparar_datos():
    if not DATA_FILE.exists():
        logging.error(f"El archivo de datos no se encontró en: {DATA_FILE}")
        raise FileNotFoundError(f"No se encontró el dataset procesado en {DATA_FILE}")
    df = pd.read_csv(DATA_FILE)
    df['PERIODO'] = df['AÑO'].astype(str) + '-Q' + df['TRIMESTRE'].astype(str)
    df = df.sort_values(['AÑO', 'TRIMESTRE']).reset_index(drop=True)
    logging.info(f"Datos cargados y preparados. {len(df)} registros encontrados.")
    return df

def calcular_datos_estatales(df):
    df_periodo = df.groupby(['AÑO', 'TRIMESTRE', 'PERIODO']).apply(lambda x: pd.Series({
        'TOTAL_INSEGUROS_PONDERADO': (x['PCT_INSEGUROS'] * x['TOTAL_REGISTROS']).sum(),
        'TOTAL_REGISTROS_ESTATAL': x['TOTAL_REGISTROS'].sum()
    })).reset_index()
    df_periodo['PCT_INSEGUROS_PONDERADO'] = (df_periodo['TOTAL_INSEGUROS_PONDERADO'] / df_periodo['TOTAL_REGISTROS_ESTATAL'])
    df_estatal = df_periodo[['PERIODO', 'AÑO', 'TRIMESTRE', 'PCT_INSEGUROS_PONDERADO']].sort_values(['AÑO', 'TRIMESTRE'])
    
    # Media Móvil
    df_estatal['MEDIA_MOVIL'] = df_estatal['PCT_INSEGUROS_PONDERADO'].rolling(window=MOVING_AVERAGE_WINDOW).mean()
    
    # Línea de Tendencia
    x = np.arange(len(df_estatal))
    y = df_estatal['PCT_INSEGUROS_PONDERADO']
    coeffs = np.polyfit(x, y, 1)
    df_estatal['TENDENCIA'] = np.polyval(coeffs, x)
    
    logging.info("Métricas estatales (promedio, media móvil, tendencia) calculadas.")
    return df_estatal.round(2)

def calcular_kpis(df_estatal, df_municipios):
    kpis = {}
    if df_estatal.empty: return kpis

    # KPIs Estatales
    ultimo = df_estatal.iloc[-1]
    kpis['ultimo_periodo'] = ultimo['PERIODO']
    kpis['ultimo_pct_estatal'] = ultimo['PCT_INSEGUROS_PONDERADO']
    
    if len(df_estatal) > 1:
        penultimo = df_estatal.iloc[-2]
        kpis['cambio_trimestral'] = ultimo['PCT_INSEGUROS_PONDERADO'] - penultimo['PCT_INSEGUROS_PONDERADO']
    else: 
        kpis['cambio_trimestral'] = 0
        
    periodo_año_anterior = df_estatal[(df_estatal['AÑO'] == ultimo['AÑO'] - 1) & (df_estatal['TRIMESTRE'] == ultimo['TRIMESTRE'])]
    if not periodo_año_anterior.empty:
        kpis['cambio_anual'] = ultimo['PCT_INSEGUROS_PONDERADO'] - periodo_año_anterior['PCT_INSEGUROS_PONDERADO'].iloc[0]
    else: 
        kpis['cambio_anual'] = 0

    # KPIs Municipales
    df_ultimo_mun = df_municipios[df_municipios['PERIODO'] == kpis['ultimo_periodo']]
    if not df_ultimo_mun.empty:
        municipio_max = df_ultimo_mun.loc[df_ultimo_mun['PCT_INSEGUROS'].idxmax()]
        municipio_min = df_ultimo_mun.loc[df_ultimo_mun['PCT_INSEGUROS'].idxmin()]
        kpis['municipio_max'] = {'nombre': municipio_max['NOM_MUN'], 'valor': municipio_max['PCT_INSEGUROS']}
        kpis['municipio_min'] = {'nombre': municipio_min['NOM_MUN'], 'valor': municipio_min['PCT_INSEGUROS']}
    
    logging.info("KPIs calculados.")
    return {k: round(v, 2) if isinstance(v, (int, float)) else v for k, v in kpis.items()}

def generar_datos_municipales(df):
    # Devuelve los datos de cada municipio en un formato listo para JSON
    datos_municipales = {}
    for municipio in df['NOM_MUN'].unique():
        df_mun = df[df['NOM_MUN'] == municipio].sort_values(['AÑO', 'TRIMESTRE'])
        datos_municipales[municipio] = {
            'periodos': df_mun['PERIODO'].tolist(),
            'valores': df_mun['PCT_INSEGUROS'].tolist()
        }
    logging.info(f"Datos para {len(datos_municipales)} municipios procesados.")
    return datos_municipales

def main():
    try:
        configurar_logging()
        logging.info("--- Iniciando Generación de Archivo de Datos JSON ---")
        
        df_completo = cargar_y_preparar_datos()
        df_estatal = calcular_datos_estatales(df_completo)
        kpis = calcular_kpis(df_estatal, df_completo)
        datos_municipales = generar_datos_municipales(df_completo)
        
        # Consolidar todos los datos en una única estructura
        output_data = {
            "fecha_generacion": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "kpis": kpis,
            "serie_estatal": {
                "periodos": df_estatal['PERIODO'].tolist(),
                "pct_inseguridad": df_estatal['PCT_INSEGUROS_PONDERADO'].tolist(),
                "media_movil": df_estatal['MEDIA_MOVIL'].tolist(),
                "tendencia": df_estatal['TENDENCIA'].tolist()
            },
            "datos_municipales": datos_municipales
        }
        
        # Guardar a JSON
        with open(JSON_OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, ensure_ascii=False, indent=4)
            
        logging.info(f"Archivo de datos guardado exitosamente en: {JSON_OUTPUT_FILE}")

    except FileNotFoundError as e:
        logging.error(f"ERROR CRÍTICO: {e}")
    except Exception as e:
        logging.error(f"Ocurrió un error inesperado: {e}", exc_info=True)
    finally:
        logging.info("--- Proceso de Generación de Datos Finalizado ---")

if __name__ == '__main__':
    main()
