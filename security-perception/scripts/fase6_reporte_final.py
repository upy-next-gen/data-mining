#!/usr/bin/env python3
"""
Fase 6: Generación de Reporte Profesional en HTML (con Gráfico de Barras)
"""
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')
import base64
from io import BytesIO
import logging
from datetime import datetime
import os

# --- CONFIGURACIÓN ---
DATASET_MAESTRO_PATH = "reports/dataset_final_yucatan.csv"
OUTPUT_HTML_PATH = "reports/reporte_percepcion_seguridad_yucatan.html"
LOG_PATH = os.path.join("logs", f"fase6_reporte_visual_{datetime.now().strftime('%Y%m%d')}.log")

# Paleta de colores
COLOR_PALETTE = {
    "space_cadet": "#25344F", "slate_gray": "#617891", "tan": "#D5B893",
    "coffee": "#6F4D38", "caput_mortuum": "#632024", "background": "#F5F5F5",
    "text_main": "#333333", "text_light": "#666666"
}

# Configuración de Matplotlib
plt.rcParams['font.family'] = 'sans-serif'
plt.rcParams['font.sans-serif'] = ['Poppins', 'Arial']
plt.rcParams['axes.labelcolor'] = COLOR_PALETTE['text_main']
plt.rcParams['xtick.color'] = COLOR_PALETTE['text_light']
plt.rcParams['ytick.color'] = COLOR_PALETTE['text_light']
plt.rcParams['figure.dpi'] = 100

def setup_logging():
    os.makedirs("logs", exist_ok=True)
    logging.basicConfig(level=logging.INFO,
                        format='[%(asctime)s] %(levelname)s: %(message)s',
                        handlers=[logging.FileHandler(LOG_PATH, mode='w', encoding='utf-8'),
                                  logging.StreamHandler()])

setup_logging()
logger = logging.getLogger(__name__)

def load_prepared_data(filepath):
    logger.info(f"Cargando dataset maestro desde: {filepath}")
    try:
        df = pd.read_csv(filepath, encoding='utf-8')
        df['PERIODO'] = df['AÑO'].astype(str) + '-Q' + df['TRIMESTRE'].astype(str)
        logger.info(f"Datos cargados exitosamente. {len(df)} registros encontrados.")
        return df
    except FileNotFoundError:
        logger.critical(f"ERROR: No se encontró el archivo {filepath}. Asegúrate de ejecutar la Fase 5 primero.")
        return None

def calculate_state_average(df):
    logger.info("Calculando promedio estatal ponderado...")
    df_safe = df[df['TOTAL_REGISTROS'] > 0].copy()
    df_safe['INSEGUROS_PONDERADOS'] = df_safe['PCT_INSEGUROS'] * df_safe['TOTAL_REGISTROS']
    grouped = df_safe.groupby('PERIODO').agg(
        SUMA_PONDERADA=('INSEGUROS_PONDERADOS', 'sum'),
        TOTAL_REGISTROS=('TOTAL_REGISTROS', 'sum')
    ).reset_index()
    grouped['PCT_INSEGUROS_ESTATAL'] = grouped['SUMA_PONDERADA'] / grouped['TOTAL_REGISTROS']
    return grouped[['PERIODO', 'PCT_INSEGUROS_ESTATAL']].sort_values('PERIODO')

def plot_to_base64(fig):
    buf = BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")
    plt.close(fig)
    return base64.b64encode(buf.getvalue()).decode('utf-8')

def generate_state_graph(df_state):
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(df_state['PERIODO'], df_state['PCT_INSEGUROS_ESTATAL'],
            marker='o', linestyle='-', color=COLOR_PALETTE['space_cadet'], linewidth=2.5)
    ax.set_title("Evolución de la Percepción de Inseguridad en Yucatán\n(Promedio Ponderado)",
                 fontsize=16, weight='bold', color=COLOR_PALETTE['text_main'])
    ax.set_ylabel("Porcentaje de Percepción de Inseguridad (%)", fontsize=12, color=COLOR_PALETTE['text_light'])
    ax.set_xlabel("Periodo (Año-Trimestre)", fontsize=12, color=COLOR_PALETTE['text_light'])
    ax.grid(True, which='both', linestyle='--', linewidth=0.5)
    ax.tick_params(axis='x', rotation=45)
    ax.set_ylim(0, max(100, df_state['PCT_INSEGUROS_ESTATAL'].max() * 1.1))
    fig.tight_layout()
    return plot_to_base64(fig)

def generate_municipal_graphs(df):
    """Genera un gráfico de líneas para cada municipio con títulos y colores corregidos."""
    avg_insecurity = df.groupby('NOM_MUN')['PCT_INSEGUROS'].mean().sort_values(ascending=False)
    graphs = []
    for municipio in avg_insecurity.index:
        df_mun = df[df['NOM_MUN'] == municipio].sort_values('PERIODO')
        avg_val = avg_insecurity[municipio]

        # Colores exactos según umbrales
        if avg_val > 50:
            line_color = "#E74C3C"      # rojo
        elif avg_val > 40:
            line_color = "#F39C12"      # amarillo/naranja
        else:
            line_color = "#1ABC9C"      # verde

        fig, ax = plt.subplots(figsize=(9, 5))
        ax.plot(df_mun['PERIODO'], df_mun['PCT_INSEGUROS'],
                marker='o', linestyle='-', color=line_color, linewidth=2)
        ax.set_title(f"Municipio: {municipio} - Evolución de Percepción de Inseguridad",
                     fontsize=14, weight='bold', color=COLOR_PALETTE['text_main'])
        ax.set_ylabel("Percepción de Inseguridad (%)", fontsize=10, color=COLOR_PALETTE['text_light'])
        ax.grid(True, which='both', linestyle='--', linewidth=0.5)
        ax.tick_params(axis='x', rotation=45)
        ax.set_ylim(0, 100)
        fig.tight_layout()

        graphs.append({
            'municipio': municipio,
            'promedio': f"{avg_val:.1f}%",
            'base64': plot_to_base64(fig)
        })
    return graphs

def create_html_table(df):
    logger.info("Creando tabla de datos pivote...")
    pivot_df = df.pivot_table(index='NOM_MUN', columns='PERIODO', values='PCT_INSEGUROS').fillna('-')
    table_html = pivot_df.to_html(classes='tabla-datos', border=0)
    logger.info("Tabla de datos creada.")
    return table_html

def generate_latest_comparison_bar_chart(df):
    logger.info("Generando gráfico de barras comparativo...")
    df_ultimo_año = df[df['AÑO'] == df['AÑO'].max()]
    df_reciente = df_ultimo_año[df_ultimo_año['TRIMESTRE'] == df_ultimo_año['TRIMESTRE'].max()]
    df_reciente = df_reciente.sort_values(by='PCT_INSEGUROS', ascending=True)

    fig, ax = plt.subplots(figsize=(10, len(df_reciente) * 0.6 + 1))
    bars = ax.barh(df_reciente['NOM_MUN'], df_reciente['PCT_INSEGUROS'],
                   color=COLOR_PALETTE['slate_gray'])

    periodo_titulo = f"Año {df_reciente['AÑO'].iloc[0]} - Trimestre {df_reciente['TRIMESTRE'].iloc[0]}"
    ax.set_title(f"Comparativo de Inseguridad Municipal ({periodo_titulo})",
                 fontsize=16, weight='bold')
    ax.set_xlabel("Porcentaje de Percepción de Inseguridad (%)", fontsize=12)
    ax.set_xlim(0, 100)

    for bar in bars:
        width = bar.get_width()
        ax.text(width + 1, bar.get_y() + bar.get_height()/2, f'{width:.1f}%', va='center')

    fig.tight_layout()
    return plot_to_base64(fig)

def generate_html_report(state_graph, bar_chart, municipal_graphs, table_html, data_period):
    logger.info("Ensamblando el reporte HTML avanzado...")
    municipal_sections = "".join([
        f"""<div class="card grafico-municipal"><h3>{g['municipio']}</h3>
        <p class="metadata-grafico">Promedio histórico de inseguridad:
        <strong>{g['promedio']}</strong></p>
        <img src="data:image/png;base64,{g['base64']}" alt="Gráfico de {g['municipio']}"></div>"""
        for g in municipal_graphs
    ])
    html_template = f"""
<!DOCTYPE html><html lang="es"><head><meta charset="UTF-8">
<title>Reporte de Percepción de Inseguridad en Yucatán</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Poppins:wght@300;400;600;700&display=swap" rel="stylesheet">
<style>
body{{font-family:'Poppins',sans-serif;margin:0;background-color:{COLOR_PALETTE['background']};color:{COLOR_PALETTE['text_main']};}}
.container{{max-width:1200px;margin:40px auto;padding:20px 40px;background-color:#FFFFFF;box-shadow:0 4px 8px rgba(0,0,0,0.1);}}
header{{border-bottom:4px solid {COLOR_PALETTE['space_cadet']};padding-bottom:20px;margin-bottom:40px;text-align:center;}}
h1{{font-size:36px;color:{COLOR_PALETTE['space_cadet']};margin-bottom:10px;}}
h2{{font-size:28px;color:{COLOR_PALETTE['coffee']};border-bottom:2px solid {COLOR_PALETTE['tan']};padding-bottom:10px;margin-top:50px;}}
h3{{font-size:20px;color:{COLOR_PALETTE['space_cadet']};margin-bottom:5px;}}
.metadata{{font-size:14px;color:{COLOR_PALETTE['text_light']};}}
.card{{background-color:#FFFFFF;padding:30px;margin:30px 0;border-left:5px solid {COLOR_PALETTE['tan']};}}
.card img{{max-width:100%;height:auto;margin-top:20px;}}
.descripcion{{font-size:16px;line-height:1.6;margin-bottom:20px;}}
.grid-municipios{{display:grid;grid-template-columns:repeat(auto-fit,minmax(400px,1fr));gap:30px;}}
.tabla-container{{overflow-x:auto;}}
.tabla-datos{{width:100%;border-collapse:collapse;margin-top:20px;}}
.tabla-datos th,.tabla-datos td{{padding:12px 15px;text-align:left;border-bottom:1px solid #DDDDDD;}}
.tabla-datos th{{background-color:{COLOR_PALETTE['slate_gray']};color:#FFFFFF;font-weight:600;}}
.tabla-datos tbody tr:nth-of-type(even){{background-color:#f9f9f9;}}
.tabla-datos tbody tr:hover{{background-color:{COLOR_PALETTE['tan']};color:{COLOR_PALETTE['coffee']};}}
#contenido ul {{list-style-type: disc; margin-left: 20px; font-size: 16px;}}
#contenido ul li {{margin-bottom: 8px;}}
#contenido a {{text-decoration: none; color: {COLOR_PALETTE['space_cadet']};}}
#contenido a:hover {{text-decoration: underline; color: #F39C12;}}
footer{{margin-top:50px;padding-top:20px;border-top:1px solid #DDDDDD;text-align:center;font-size:12px;color:{COLOR_PALETTE['text_light']};}}
</style></head>
<body>
<div class="container">
<header>
<h1>Reporte de Percepción de Seguridad</h1>
<p class="metadata">Análisis de la Evolución en Yucatán | Fuente: ENSU (INEGI)</p>
<p class="metadata">Generado en: Septimebre de 2025 | Periodo analizado: {data_period}</p>
</header>

<!-- SECCIÓN DE CONTENIDO -->
<section id="contenido">
<h2>Contenido</h2>
<ul>
    <li><a href="#estatal">Visión General del Estado</a></li>
    <li><a href="#comparativo">Comparativo del Periodo Más Reciente</a></li>
    <li><a href="#municipios">Análisis Detallado por Municipio</a></li>
    <li><a href="#tabla">Tabla de Datos Completa</a></li>
</ul>
</section>

<section id="estatal">
<h2>Visión General del Estado</h2>
<div class="card">
<p class="descripcion">
El siguiente gráfico muestra la tendencia general de la percepción de inseguridad
para todo el estado de Yucatán. El valor de cada punto representa un <strong>promedio ponderado</strong>.
</p>
<img src="data:image/png;base64,{state_graph}" alt="Gráfico Estatal de Inseguridad">
</div></section>

<section id="comparativo">
<h2>Comparativo del Periodo Más Reciente</h2>
<div class="card">
<p class="descripcion">
Este gráfico de barras compara directamente el nivel de percepción de inseguridad
entre los municipios durante el último trimestre disponible, permitiendo una rápida
identificación de las áreas con mayor y menor preocupación ciudadana en la actualidad.
</p>
<img src="data:image/png;base64,{bar_chart}" alt="Gráfico de Barras Comparativo">
</div></section>

<section id="municipios">
<h2>Análisis Detallado por Municipio</h2>
<p class="descripcion">
A continuación, se presenta la evolución para cada municipio. Están ordenados
de mayor a menor según su promedio histórico de inseguridad.
</p>
<div class="grid-municipios">{municipal_sections}</div>
</section>

<section id="tabla">
<h2>Tabla de Datos Completa</h2>
<p class="descripcion">
La siguiente tabla resume el porcentaje de percepción de inseguridad para cada municipio en cada periodo.
</p>
<div class="tabla-container card">{table_html}</div>
</section>

<footer><p>&copy 2025 Karen Cardiel | Data Mining</p></footer>
</div></body></html>
    """
    with open(OUTPUT_HTML_PATH, 'w', encoding='utf-8') as f:
        f.write(html_template)
    logger.info(f"Reporte HTML guardado exitosamente en: {OUTPUT_HTML_PATH}")

def main():
    logger.info("--- INICIANDO FASE 6: GENERACIÓN DE REPORTE VISUAL ---")
    df_master = load_prepared_data(DATASET_MAESTRO_PATH)
    if df_master is None or df_master.empty:
        return

    data_period = f"{df_master['AÑO'].min()} - {df_master['AÑO'].max()}"
    df_state = calculate_state_average(df_master)
    state_graph_b64 = generate_state_graph(df_state)
    municipal_graphs_list = generate_municipal_graphs(df_master)
    table_html_str = create_html_table(df_master)
    bar_chart_b64 = generate_latest_comparison_bar_chart(df_master)
    generate_html_report(state_graph_b64, bar_chart_b64,
                         municipal_graphs_list, table_html_str, data_period)
    logger.info("--- PROCESO DE REPORTE FINALIZADO EXITOSAMENTE ---")

if __name__ == "__main__":
    main()
