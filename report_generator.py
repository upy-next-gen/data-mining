import pandas as pd
import glob
import os
from pathlib import Path

# --- CONFIGURACIÓN ---
DATA_DIR = Path("data/yucatan-inseguridad")
OUTPUT_FILE = "reporte_inseguridad_v3.html"
ASSET_PATH = "assets/renacimiento-maya-logo.svg"
GOV_CHANGE_YEAR = 2024
GOV_CHANGE_QUARTER = 4

# --- FUNCIONES DE CÁLCULO ---

def load_and_prepare_data():
    """Carga, consolida y prepara los datos de Yucatán."""
    csv_files = glob.glob(os.path.join(DATA_DIR, "procesado_*.csv"))
    if not csv_files:
        return None
    
    df = pd.concat((pd.read_csv(f) for f in csv_files), ignore_index=True)
    
    df_yuc = df[df['NOM_ENT'] == 'YUCATAN'].copy()
    if df_yuc.empty:
        return None

    df_yuc['TRIMESTRE_NUM'] = df_yuc['TRIMESTRE'].str.replace('Q', '').astype(int)
    df_yuc['FECHA'] = pd.to_datetime(df_yuc['AÑO'].astype(str) + '-' + (df_yuc['TRIMESTRE_NUM'] * 3 - 2).astype(str) + '-01')
    df_yuc = df_yuc.sort_values('FECHA').drop_duplicates(subset=['NOM_MUN', 'FECHA'])
    
    return df_yuc

def calculate_kpis(df):
    """Calcula los KPIs principales."""
    df_antes = df[(df['AÑO'] < GOV_CHANGE_YEAR) | ((df['AÑO'] == GOV_CHANGE_YEAR) & (df['TRIMESTRE_NUM'] < GOV_CHANGE_QUARTER))]
    df_despues = df[(df['AÑO'] > GOV_CHANGE_YEAR) | ((df['AÑO'] == GOV_CHANGE_YEAR) & (df['TRIMESTRE_NUM'] >= GOV_CHANGE_QUARTER))]

    avg_inseguridad_antes = (df_antes['TOTAL_INSEGUROS'].sum() / df_antes['TOTAL_REGISTROS'].sum()) * 100 if not df_antes.empty and df_antes['TOTAL_REGISTROS'].sum() > 0 else 0
    avg_inseguridad_despues = (df_despues['TOTAL_INSEGUROS'].sum() / df_despues['TOTAL_REGISTROS'].sum()) * 100 if not df_despues.empty and df_despues['TOTAL_REGISTROS'].sum() > 0 else 0
    
    cambio_neto = avg_inseguridad_despues - avg_inseguridad_antes

    df_ultimo_trimestre = df[df['FECHA'] == df['FECHA'].max()]
    municipio_mas_inseguro = df_ultimo_trimestre.loc[df_ultimo_trimestre['PCT_INSEGUROS'].idxmax()] if not df_ultimo_trimestre.empty else None

    return {
        "avg_antes": avg_inseguridad_antes,
        "avg_despues": avg_inseguridad_despues,
        "cambio_neto": cambio_neto,
        "municipio_top": municipio_mas_inseguro
    }

def get_timeseries_data(df):
    """Prepara datos para la gráfica de línea."""
    ts_df = df.groupby('FECHA').apply(lambda x: (x['TOTAL_INSEGUROS'].sum() / x['TOTAL_REGISTROS'].sum()) * 100 if x['TOTAL_REGISTROS'].sum() > 0 else 0).reset_index(name='PCT_INSEGUROS')
    return ts_df.sort_values('FECHA')

# --- FUNCIONES DE GENERACIÓN DE HTML ---

def generate_svg_line_chart(ts_df):
    """Genera el código SVG para la gráfica de línea con eje Y ajustado."""
    if ts_df.empty: return ""
    
    width, height = 1100, 450
    padding_left, padding_right, padding_top, padding_bottom = 70, 20, 50, 70
    
    # Lógica de escala del eje Y para exagerar la visualización
    data_max = ts_df['PCT_INSEGUROS'].max()
    data_min = ts_df['PCT_INSEGUROS'].min()
    data_range = data_max - data_min
    y_axis_top = data_max + data_range * 0.15
    y_axis_bottom = max(0, data_min - data_range * 0.15)
    y_range = y_axis_top - y_axis_bottom

    if y_range == 0: y_range = 1 # Evitar división por cero

    points = []
    for i, row in ts_df.iterrows():
        x = padding_left + i * (width - padding_left - padding_right) / (len(ts_df) - 1)
        y = padding_top + (1 - ((row['PCT_INSEGUROS'] - y_axis_bottom) / y_range)) * (height - padding_top - padding_bottom)
        points.append(f"{x},{y}")
    
    path_d = "M " + " L ".join(points)
    
    gov_change_date = pd.to_datetime(f'{GOV_CHANGE_YEAR}-{(GOV_CHANGE_QUARTER * 3 - 2)}-01')
    change_indices = ts_df[ts_df['FECHA'] >= gov_change_date].index
    change_x = -1
    if not change_indices.empty:
        change_point_index = change_indices.min()
        change_x = padding_left + change_point_index * (width - padding_left - padding_right) / (len(ts_df) - 1)

    y_labels = ""
    for i in range(6):
        val = y_axis_bottom + i * (y_range / 5)
        y = padding_top + (1 - (i * y_range / 5) / y_range) * (height - padding_top - padding_bottom)
        y_labels += f'<text x="{padding_left - 10}" y="{y}" fill="#000000" text-anchor="end" alignment-baseline="middle">{val:.1f}%</text>'

    x_labels = ""
    for i, row in ts_df.iterrows():
        x = padding_left + i * (width - padding_left - padding_right) / (len(ts_df) - 1)
        label = f"{row['FECHA'].year} Q{row['FECHA'].quarter}"
        x_labels += f'<text x="{x}" y="{height - padding_bottom + 15}" transform="rotate(-45, {x}, {height - padding_bottom + 15})" fill="#000000" text-anchor="end">{label}</text>'

    gov_change_line = ''
    if change_x != -1:
        gov_change_line = f'''
        <line x1="{change_x}" y1="{padding_top-10}" x2="{change_x}" y2="{height - padding_bottom + 10}" stroke="#8C1A33" stroke-width="2" stroke-dasharray="4"/>
        <text x="{change_x + 10}" y="{padding_top}" fill="#8C1A33" font-weight="bold">Nuevo Gobierno</text>
        '''

    return f'''
    <svg width="{width}" height="{height}" viewbox="0 0 {width} {height}" style="background-color: #FFFFFF; border-radius: 8px; padding: 10px;">
        {gov_change_line}
        {y_labels}
        {x_labels}
        <path d="{path_d}" fill="none" stroke="#8C1A33" stroke-width="3" />
        {' '.join([f'<circle cx="{p.split(",")[0]}" cy="{p.split(",")[1]}" r="4" fill="#8C1A33" />' for p in points])}
    </svg>
    '''

def generate_breakdown_section(df_quarter_data):
    """Genera una sección de desglose para un trimestre específico."""
    if df_quarter_data.empty: return ""

    year = df_quarter_data["AÑO"].iloc[0]
    quarter = df_quarter_data["TRIMESTRE_NUM"].iloc[0]

    total_seguros = df_quarter_data['TOTAL_SEGUROS'].sum()
    total_inseguros = df_quarter_data['TOTAL_INSEGUROS'].sum()
    total_no_resp = df_quarter_data['TOTAL_NO_RESPONDE'].sum()
    total_gen = total_seguros + total_inseguros + total_no_resp

    pct_seg = (total_seguros / total_gen) * 100 if total_gen > 0 else 0
    pct_inseg = (total_inseguros / total_gen) * 100 if total_gen > 0 else 0
    
    angle_seg = pct_seg * 3.6
    angle_inseg = pct_inseg * 3.6

    donut_chart = f'''
    <div class="donut-chart-container">
        <div class="donut-chart" style="background: conic-gradient(#C49A6C 0deg {angle_inseg}deg, #E3C49A {angle_inseg}deg {angle_inseg + angle_seg}deg, #8C1A33 {angle_inseg + angle_seg}deg 360deg);">
            <div class="donut-center">
                <span>Promedio Yucatán</span>
            </div>
        </div>
        <div class="donut-legend">
            <div class="legend-item"><span class="color-box" style="background-color: #C49A6C;"></span>Inseguro ({pct_inseg:.1f}%)</div>
            <div class="legend-item"><span class="color-box" style="background-color: #E3C49A;"></span>Seguro ({pct_seg:.1f}%)</div>
            <div class="legend-item"><span class="color-box" style="background-color: #8C1A33;"></span>No Responde</div>
        </div>
    </div>
    '''

    bars_html = '<div class="bar-chart-container">'
    max_pct = df_quarter_data['PCT_INSEGUROS'].max() if not df_quarter_data.empty else 0
    
    for _, row in df_quarter_data.sort_values('PCT_INSEGUROS', ascending=False).iterrows():
        bar_width = (row['PCT_INSEGUROS'] / max_pct) * 100 if max_pct > 0 else 0
        bars_html += f'''
        <div class="bar-item">
            <span class="bar-label">{row["NOM_MUN"]}</span>
            <div class="bar-wrapper">
                <div class="bar" style="width: {bar_width}%; background-color: #C49A6C;">
                    <span>{row["PCT_INSEGUROS"]:.1f}%</span>
                </div>
            </div>
        </div>
        '''
    bars_html += '</div>'

    return f'''
    <h2>Desglose - {year} Q{quarter}</h2>
    <div class="chart-section dual-panel">
        {donut_chart}
        {bars_html}
    </div>
    '''

# --- GENERACIÓN DEL REPORTE PRINCIPAL ---

def create_report():
    """Función principal que genera el reporte HTML revisado."""
    df = load_and_prepare_data()
    if df is None:
        print("No se encontraron datos procesados o no hay datos de Yucatán. No se puede generar el reporte.")
        return

    kpis = calculate_kpis(df)
    ts_data = get_timeseries_data(df)
    
    unique_quarters = df['FECHA'].unique()
    last_4_quarters = sorted(unique_quarters, reverse=True)[:4]
    breakdown_sections_html = ""
    for quarter_date in last_4_quarters:
        df_quarter_data = df[df['FECHA'] == quarter_date]
        breakdown_sections_html += generate_breakdown_section(df_quarter_data)

    svg_chart = generate_svg_line_chart(ts_data)
    
    cambio_neto_class = "kpi-delta-pos" if kpis["cambio_neto"] < 0 else "kpi-delta-neg"
    cambio_neto_symbol = "▼" if kpis["cambio_neto"] < 0 else "▲"

    html_template = f'''
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Reporte de Percepción de Inseguridad en Yucatán</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Montserrat:wght@400;700;900&display=swap" rel="stylesheet">
    <style>
        body {{ font-family: 'Montserrat', sans-serif; background-color: #720025; color: #F5F5F5; margin: 0; padding: 20px; }}
        .container {{ max-width: 1200px; margin: auto; }}
        header {{ display: flex; align-items: center; gap: 20px; border-bottom: 2px solid #E3C49A; padding-bottom: 20px; margin-bottom: 30px; }}
        header img {{ height: 120px; }}
        header h1 {{ color: #FFFFFF; font-size: 2.5em; font-weight: 900; margin: 0; }}
        h2 {{ color: #FFFFFF; font-size: 1.8em; font-weight: 700; border-left: 5px solid #C49A6C; padding-left: 15px; margin-top: 40px; }}
        .kpi-container {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 20px; margin-bottom: 30px; }}
        .kpi-card {{ background-color: #F5F5F5; color: #1A1A1A; padding: 20px; border-radius: 8px; border: 1px solid #E3C49A; }}
        .kpi-title {{ font-size: 0.9em; color: #1A1A1A; opacity: 0.7; margin-bottom: 10px; }}
        .kpi-value {{ font-size: 2.2em; font-weight: 700; color: #1A1A1A; }}
        .kpi-delta-pos {{ color: #4CAF50 !important; }}
        .kpi-delta-neg {{ color: #F44336 !important; }}
        .kpi-subtext {{ font-size: 0.8em; color: #8C1A33; }}
        .chart-section {{ background-color: #8C1A33; padding: 20px; border-radius: 8px; margin-top: 20px; }}
        .main-chart-container {{ background-color: #FFFFFF; padding: 20px; border-radius: 8px; margin-top: 20px; }}
        .dual-panel {{ display: grid; grid-template-columns: 1fr 2fr; gap: 30px; align-items: center; }}
        .donut-chart-container {{ display: flex; flex-direction: column; align-items: center; gap: 20px; color: #F5F5F5; }}
        .donut-chart {{ width: 200px; height: 200px; border-radius: 50%; position: relative; }}
        .donut-center {{ position: absolute; top: 50%; left: 50%; transform: translate(-50%, -50%); width: 120px; height: 120px; background: #8C1A33; border-radius: 50%; display: flex; flex-direction: column; justify-content: center; align-items: center; text-align: center; font-weight: 700; }}
        .donut-legend {{ display: flex; flex-direction: column; gap: 10px; }}
        .legend-item {{ display: flex; align-items: center; gap: 10px; }}
        .color-box {{ width: 20px; height: 20px; border-radius: 4px; }}
        .bar-chart-container {{ display: flex; flex-direction: column; gap: 15px; color: #F5F5F5; }}
        .bar-item {{ display: grid; grid-template-columns: 100px 1fr; gap: 10px; align-items: center; }}
        .bar-label {{ font-weight: 700; text-align: right; }}
        .bar-wrapper {{ background-color: #720025; border-radius: 4px; padding: 2px; }}
        .bar {{ height: 25px; border-radius: 2px; display: flex; align-items: center; justify-content: flex-end; color: #1A1A1A; font-weight: 700; font-size: 0.9em; padding-right: 5px; }}
    </style>
</head>
<body>
    <div class="container">
        <header>
            <img src="{ASSET_PATH}" alt="Logo Renacimiento Maya">
            <h1>Análisis de Percepción de Inseguridad en Yucatán</h1>
        </header>

        <main>
            <h2>Indicadores Clave de Desempeño (KPIs)</h2>
            <div class="kpi-container">
                <div class="kpi-card">
                    <div class="kpi-title">Inseguridad Promedio (Antes de Q4 2024)</div>
                    <div class="kpi-value">{kpis["avg_antes"]:.2f}%</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-title">Inseguridad Promedio (Nuevo Gobierno)</div>
                    <div class="kpi-value">{kpis["avg_despues"]:.2f}%</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-title">Cambio Neto</div>
                    <div class="kpi-value {cambio_neto_class}">{cambio_neto_symbol} {abs(kpis["cambio_neto"]):.2f}%</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-title">Municipio con Mayor Inseguridad (Últ. Trimestre)</div>
                    <div class="kpi-value">{kpis["municipio_top"]["NOM_MUN"] if kpis["municipio_top"] is not None else "N/A"}</div>
                    <div class="kpi-subtext">{kpis["municipio_top"]["PCT_INSEGUROS"]:.1f}%</div>
                </div>
            </div>

            <h2>Evolución de la Percepción de Inseguridad</h2>
            <div class="main-chart-container">
                {svg_chart}
            </div>

            {breakdown_sections_html}
        </main>
    </div>
</body>
</html>
    '''
    
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        f.write(html_template)
    
    print(f"Reporte final generado exitosamente en: {OUTPUT_FILE}")

if __name__ == "__main__":
    create_report()