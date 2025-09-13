import os
import json
import logging
import pandas as pd
import glob
from datetime import datetime
import plotly.graph_objects as go
import plotly.io as pio

# --- Configuration ---
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
PROCESSED_DIR = os.path.join(BASE_DIR, 'data', 'yucatan_processed')
REPORTS_DIR = os.path.join(BASE_DIR, 'reports')
IMAGE_DIR = os.path.join(REPORTS_DIR, 'graficas')
STATS_JSON = os.path.join(REPORTS_DIR, 'estadisticas_finales.json')
OUTPUT_MD = os.path.join(REPORTS_DIR, 'reporte_percepcion.md')
LOG_FILE = os.path.join(BASE_DIR, 'logs', f"fase5_reporte_md_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

# --- Plotly Setup ---
pio.templates.default = "plotly_white"
TEAL_COLOR = '#20c997'

# --- Setup Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, mode='w'),
        logging.StreamHandler()
    ]
)

def create_line_chart(df, output_path):
    logging.info("Generating time-series line chart...")
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df['PERIODO'],
        y=df['PORCENTAJE_INSEGUROS'],
        mode='lines+markers',
        name='Inseguridad',
        marker=dict(color=TEAL_COLOR)
    ))
    fig.update_layout(
        title='Evolución de la Percepción de Inseguridad en Yucatán',
        xaxis_title='Periodo',
        yaxis_title='Porcentaje de Inseguridad Promedio'
    )
    fig.write_image(output_path)
    logging.info(f"Line chart saved to {output_path}")

def create_bar_chart(df, output_path):
    logging.info("Generating municipality bar chart...")
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df['PORCENTAJE_INSEGUROS'],
        y=df['NOM_MUN'],
        orientation='h',
        marker=dict(color=TEAL_COLOR)
    ))
    fig.update_layout(
        title='Inseguridad Promedio por Municipio',
        xaxis_title='Inseguridad Promedio (%)',
        yaxis=dict(autorange="reversed", automargin=True)
    )
    fig.write_image(output_path)
    logging.info(f"Bar chart saved to {output_path}")

def main():
    logging.info("--- Starting Phase 5: Markdown Report Generation ---")

    os.makedirs(IMAGE_DIR, exist_ok=True)

    # 1. Load data
    csv_files = glob.glob(os.path.join(PROCESSED_DIR, '*.csv'))
    if not csv_files:
        logging.error(f"No processed files found in {PROCESSED_DIR}.")
        return
    df = pd.concat((pd.read_csv(f) for f in csv_files), ignore_index=True)
    with open(STATS_JSON, 'r', encoding='utf-8') as f:
        stats = json.load(f)['estadisticas_globales']

    # 2. Prepare data for charts and analysis
    line_df = df.groupby('PERIODO')[['PORCENTAJE_INSEGUROS']].mean().sort_index().reset_index()
    bar_df = df.groupby('NOM_MUN')[['PORCENTAJE_INSEGUROS']].mean().sort_values(by='PORCENTAJE_INSEGUROS', ascending=True).reset_index()
    
    # 3. Generate static charts
    create_line_chart(line_df, os.path.join(IMAGE_DIR, "line_chart.svg"))
    create_bar_chart(bar_df, os.path.join(IMAGE_DIR, "bar_chart.svg"))

    # 4. Prepare content for Markdown
    fecha_generacion = datetime.now().strftime('%d de %B de %Y')
    lugar_mas_inseguro = bar_df.iloc[-1]
    lugar_mas_seguro = bar_df.iloc[0]
    
    table_df = df.rename(columns={
        'NOM_MUN': 'Municipio',
        'PORCENTAJE_INSEGUROS': '% Inseguridad',
        'PERIODO': 'Periodo'
    })[['Municipio', '% Inseguridad', 'Periodo']]

    # 5. Assemble Markdown content
    md_content = f"# Reporte de Percepción de Seguridad en Yucatán\n\n"
    md_content += f"*Fecha de generación: {fecha_generacion}*\n\n"
    md_content += "## Introducción\n"
    md_content += "Este reporte presenta un análisis de la percepción de seguridad en el estado de Yucatán, basado en los datos históricos de la Encuesta Nacional de Seguridad Pública Urbana (ENSU) del INEGI. El objetivo es consolidar y visualizar las tendencias a lo largo del tiempo y comparar los niveles de percepción de inseguridad entre los diferentes municipios del estado para los cuales se encontraron datos.\n\n"
    md_content += "## Resumen Ejecutivo\n\n"
    md_content += f"> **Municipio con Mayor Inseguridad (Promedio):** {lugar_mas_inseguro['NOM_MUN']} con un **{lugar_mas_inseguro['PORCENTAJE_INSEGUROS']:.2f}%**.\n"
    md_content += f"> **Municipio con Menor Inseguridad (Promedio):** {lugar_mas_seguro['NOM_MUN']} con un **{lugar_mas_seguro['PORCENTAJE_INSEGUROS']:.2f}%**.\n\n"
    md_content += "A continuación, se presentan los análisis detallados que sustentan estos resultados.\n\n"
    md_content += "## Evolución de la Percepción de Inseguridad en Yucatán\n\n"
    md_content += "El siguiente gráfico de líneas muestra cómo ha variado el promedio estatal de la percepción de inseguridad a lo largo de los trimestres. Este promedio se calcula a partir de los datos de todos los municipios disponibles en cada periodo. La visualización permite identificar fácilmente tendencias, picos o valles en la percepción de seguridad de los ciudadanos en el estado.\n\n"
    md_content += "![Evolución de Inseguridad](graficas/line_chart.svg)\n\n"
    md_content += "## Comparativa de Inseguridad por Municipio\n\n"
    md_content += "Para entender las diferencias regionales dentro del estado, el siguiente gráfico de barras clasifica cada municipio según su porcentaje promedio de percepción de inseguridad, calculado a partir de todos los periodos analizados. Esto permite una comparación directa y clara sobre qué municipios son percibidos como más o menos seguros por sus habitantes a lo largo del tiempo.\n\n"
    md_content += "![Comparativa por Municipio](graficas/bar_chart.svg)\n\n"
    md_content += "## Datos Detallados\n\n"
    md_content += "La siguiente tabla contiene los datos agregados por municipio y periodo que fueron utilizados para generar las visualizaciones anteriores. Puede ser utilizada para consultas específicas o para un análisis más profundo. \n\n"
    md_content += table_df.to_markdown(index=False)
    md_content += "\n\n## Conclusión\n\n"
    md_content += f"El análisis de los datos de la ENSU desde 2016 hasta 2025 revela una notable variación en la percepción de inseguridad tanto a nivel estatal como municipal en Yucatán. Con un promedio general de inseguridad del **{stats['promedio_pct_inseguros']:.2f}%**, se observa que **{lugar_mas_inseguro['NOM_MUN']}** es consistentemente percibido como el municipio más inseguro, mientras que **{lugar_mas_seguro['NOM_MUN']}** mantiene la percepción de mayor seguridad.\n\n"
    md_content += "La tendencia histórica muestra fluctuaciones significativas, indicando que la percepción ciudadana es sensible a eventos y contextos temporales específicos. Este reporte consolida dicha información, proveyendo una herramienta valiosa para el análisis y la toma de decisiones informadas sobre seguridad pública en la región."

    # 6. Write Markdown file
    with open(OUTPUT_MD, 'w', encoding='utf-8') as f:
        f.write(md_content)
    
    logging.info(f"Successfully created Markdown report: {OUTPUT_MD}")
    logging.info("--- Markdown report generation finished ---")

if __name__ == "__main__":
    main()
