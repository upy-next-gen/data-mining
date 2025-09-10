#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Generador simplificado de reportes ENSU para Yucatán.
Este script genera un HTML básico con los gráficos de percepción de seguridad.
"""

import os
import sys
import glob
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import base64
from io import BytesIO
from datetime import datetime

# Configuración de matplotlib
plt.style.use('ggplot')
plt.rcParams['figure.figsize'] = (10, 6)
plt.rcParams['font.size'] = 12

def cargar_datos_procesados(directorio_procesados: str) -> pd.DataFrame:
    """Carga todos los archivos CSV procesados y los combina."""
    patron_archivos = os.path.join(directorio_procesados, "procesados", "procesado_*.csv")
    archivos = glob.glob(patron_archivos)
    
    if not archivos:
        print(f"No se encontraron archivos procesados en {directorio_procesados}/procesados/")
        sys.exit(1)
    
    print(f"Se encontraron {len(archivos)} archivos procesados")
    
    dfs = []
    for archivo in archivos:
        try:
            df = pd.read_csv(archivo)
            dfs.append(df)
            print(f"Archivo cargado: {archivo}")
        except Exception as e:
            print(f"Error al cargar {archivo}: {str(e)}")
    
    if not dfs:
        print("No se pudo cargar ningún archivo de datos")
        sys.exit(1)
    
    # Combinar todos los DataFrames
    df_combinado = pd.concat(dfs, ignore_index=True)
    
    # Crear columna de año-trimestre para ordenar correctamente
    df_combinado["ORDEN_AÑO_TRIMESTRE"] = df_combinado.apply(
        lambda x: f"{x['AÑO']:04d}-{x['TRIMESTRE']}", axis=1
    )
    
    return df_combinado

def calcular_promedio_estatal(df: pd.DataFrame) -> tuple:
    """Calcula el promedio estatal ponderado por periodo."""
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
    
    return df_estatal, promedio_historico

def generar_grafico_estatal(df_estatal: pd.DataFrame, promedio_historico: float) -> str:
    """Genera un gráfico de línea con la evolución estatal de percepción de inseguridad."""
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

def generar_html_simple(imagen_estatal: str, df_estatal: pd.DataFrame, promedio_historico: float) -> str:
    """Genera un HTML simple con el gráfico estatal."""
    periodos = df_estatal["PERIODO"].tolist()
    rango_periodos = f"{periodos[0]} a {periodos[-1]}" if len(periodos) > 1 else periodos[0]
    fecha_generacion = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    
    html = f"""<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Reporte de Percepción de Seguridad en Yucatán</title>
    <style>
        body {{
            font-family: Arial, sans-serif;
            line-height: 1.6;
            color: #333;
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
        }}
        
        h1, h2 {{
            color: #2c3e50;
        }}
        
        .img-container {{
            text-align: center;
            margin: 2em 0;
        }}
        
        img {{
            max-width: 100%;
            height: auto;
        }}
        
        @media (max-width: 768px) {{
            body {{
                padding: 10px;
            }}
        }}
    </style>
</head>
<body>
    <h1>Reporte de Percepción de Seguridad en Yucatán</h1>
    <p>Periodo analizado: {rango_periodos}</p>
    <p>Fecha de generación: {fecha_generacion}</p>
    <p>Fuente: INEGI - Encuesta Nacional de Seguridad Pública Urbana (ENSU)</p>
    
    <h2>Promedio Estatal</h2>
    <p>El promedio estatal de percepción de inseguridad en Yucatán para el periodo 
    analizado es de <strong>{promedio_historico}%</strong>.</p>
    
    <div class="img-container">
        <img src="data:image/png;base64,{imagen_estatal}" 
            alt="Gráfico de evolución estatal">
    </div>
    
    <h2>Metodología</h2>
    <p>Este reporte se basa en datos de la Encuesta Nacional de Seguridad Pública Urbana (ENSU) 
    del INEGI, con la pregunta "BP1_1: ¿Se siente seguro(a) en su municipio?", 
    donde las respuestas se codifican como: 1=Seguro, 2=Inseguro, 9=No responde.</p>
</body>
</html>
"""
    
    return html

def main():
    """Función principal."""
    dir_procesados = ".\\data\\yucatan-inseguridad"
    dir_reportes = ".\\data\\yucatan-inseguridad\\reportes"
    
    try:
        # Asegurarse de que exista el directorio de reportes
        os.makedirs(dir_reportes, exist_ok=True)
        
        # Cargar datos procesados
        df = cargar_datos_procesados(dir_procesados)
        
        # Calcular promedio estatal
        df_estatal, promedio_historico = calcular_promedio_estatal(df)
        
        # Generar gráfico estatal
        imagen_estatal = generar_grafico_estatal(df_estatal, promedio_historico)
        
        # Generar HTML simple
        html = generar_html_simple(imagen_estatal, df_estatal, promedio_historico)
        
        # Guardar reporte HTML
        reporte_path = os.path.join(dir_reportes, "reporte_yucatan_simple.html")
        with open(reporte_path, "w", encoding="utf-8") as f:
            f.write(html)
        
        print(f"\nReporte simple generado exitosamente en: {reporte_path}")
        
    except Exception as e:
        print(f"Error durante la generación del reporte: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
