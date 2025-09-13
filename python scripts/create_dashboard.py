import pandas as pd
import json
import os

# --- Configuration ---
DATA_FILE = '/home/danielgomez/Documents/data_mining/data-mining/reporte/consolidado_yucatan.csv'
OUTPUT_DIR = '/home/danielgomez/Documents/data_mining/data-mining/reporte'
HTML_FILE = os.path.join(OUTPUT_DIR, 'dashboard.html')
CSS_FILE = os.path.join(OUTPUT_DIR, 'style.css')
DATA_JS_FILE = os.path.join(OUTPUT_DIR, 'dashboard_data.js')
LOGIC_JS_FILE = os.path.join(OUTPUT_DIR, 'dashboard_logic.js')
TREND_IMAGE_FILE = 'tendencia_inseguridad.png'

def generate_css():
    css_content = """
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif; background-color: #f4f7f6; color: #333; margin: 0; padding: 20px; }
    .container { max-width: 95%; margin: auto; background: #fff; padding: 20px; border-radius: 8px; box-shadow: 0 0 15px rgba(0,0,0,0.05); }
    h1, h2 { color: #2c3e50; border-bottom: 2px solid #e0e0e0; padding-bottom: 10px; }
    .kpi-container { display: flex; justify-content: space-around; margin-bottom: 20px; flex-wrap: wrap; gap: 10px; }
    .kpi-card { background: #ecf0f1; padding: 20px; border-radius: 8px; text-align: center; flex-grow: 1; min-width: 250px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
    .kpi-card h3 { margin: 0; color: #3498db; }
    .kpi-card p { font-size: 24px; font-weight: bold; margin: 10px 0 0 0; }
    .chart-container { margin-top: 30px; }
    #period-selector { padding: 8px; font-size: 16px; margin-bottom: 15px; }
    table { width: 100%; border-collapse: collapse; margin-top: 20px; }
    th, td { padding: 12px; border: 1px solid #ddd; text-align: left; }
    th { background-color: #3498db; color: white; }
    tbody tr:nth-child(even) { background-color: #f2f2f2; }
    img { max-width: 100%; height: auto; border-radius: 8px; margin-top: 15px; }
    """
    with open(CSS_FILE, 'w', encoding='utf-8') as f:
        f.write(css_content)
    print(f"CSS file created at: {CSS_FILE}")

def generate_js_files(df):
    # --- Prepare and write data file ---
    df['PERIODO'] = df['AÑO'].astype(str) + '-T' + df['TRIMESTRE'].astype(str)
    periods = sorted(df['PERIODO'].unique().tolist())
    data_for_js = {}
    for period in periods:
        period_df = df[df['PERIODO'] == period].sort_values('PCT_INSEGUROS', ascending=False)
        data_for_js[period] = {
            'labels': period_df['NOM_MUN'].tolist(),
            'data': period_df['PCT_INSEGUROS'].round(2).tolist()
        }
    with open(DATA_JS_FILE, 'w', encoding='utf-8') as f:
        f.write(f"const dashboardData = {json.dumps(data_for_js, indent=4)};")
    print(f"JS data file created at: {DATA_JS_FILE}")

    # --- Write logic file ---
    latest_period = periods[-1]
    logic_content = f"""
    document.addEventListener('DOMContentLoaded', function () {{
        // --- Chart Logic ---
        const selector = document.getElementById('period-selector');
        const ctx = document.getElementById('rankingChart').getContext('2d');
        let rankingChart;

        const periods = Object.keys(dashboardData);
        periods.forEach(p => {{
            const option = document.createElement('option');
            option.value = p;
            option.textContent = p;
            selector.appendChild(option);
        }});

        function updateChart(period) {{
            const chartData = dashboardData[period];
            if (rankingChart) {{
                rankingChart.destroy();
            }}
            rankingChart = new Chart(ctx, {{
                type: 'bar',
                data: {{
                    labels: chartData.labels,
                    datasets: [{{ 
                        label: '% de Percepción de Inseguridad',
                        data: chartData.data,
                        backgroundColor: 'rgba(52, 152, 219, 0.7)',
                        borderColor: 'rgba(41, 128, 185, 1)',
                        borderWidth: 1
                    }}]
                }},
                options: {{
                    indexAxis: 'y',
                    scales: {{ x: {{ beginAtZero: true, max: 100 }} }},
                    plugins: {{ legend: {{ display: false }} }}
                }}
            }});
        }}

        selector.addEventListener('change', (event) => {{
            updateChart(event.target.value);
        }});

        selector.value = '{latest_period}';
        updateChart('{latest_period}');

        // --- Table Filtering Logic ---
        const yearFilter = document.getElementById('year-filter');
        const tableRows = document.querySelectorAll('#data-table tbody tr');

        yearFilter.addEventListener('change', (event) => {{
            const selectedYear = event.target.value;
            tableRows.forEach(row => {{
                const rowYear = row.getAttribute('data-year');
                if (selectedYear === 'all' || rowYear === selectedYear) {{
                    row.style.display = '';
                }} else {{
                    row.style.display = 'none';
                }}
            }});
        }});
    }});
    """
    with open(LOGIC_JS_FILE, 'w', encoding='utf-8') as f:
        f.write(logic_content)
    print(f"JS logic file created at: {LOGIC_JS_FILE}")

def generate_html_file(df):
    df['PERIODO'] = df['AÑO'].astype(str) + '-T' + df['TRIMESTRE'].astype(str)
    periods = sorted(df['PERIODO'].unique().tolist())
    years = sorted(df['AÑO'].unique().tolist())
    latest_period = periods[-1]
    kpi_df = df[df['PERIODO'] == latest_period]
    total_inseguros = kpi_df['TOTAL_INSEGUROS'].sum()
    total_registros = kpi_df['TOTAL_REGISTROS'].sum()
    overall_insecurity_pct = (total_inseguros / total_registros * 100) if total_registros > 0 else 0
    highest_insecurity_row = kpi_df.loc[kpi_df['PCT_INSEGUROS'].idxmax()]
    lowest_insecurity_row = kpi_df.loc[kpi_df['PCT_INSEGUROS'].idxmin()]

    # --- Manual Table Generation ---
    table_header = "".join([f'<th>{col}</th>' for col in df.columns])
    table_rows = ""
    for _, row in df.iterrows():
        table_rows += f'<tr data-year="{row["AÑO"]}">' + "".join([f'<td>{val}</td>' for val in row]) + '</tr>'
    table_html = f'''
    <table class="table" id="data-table">
        <thead><tr>{table_header}</tr></thead>
        <tbody>{table_rows}</tbody>
    </table>
    '''

    # --- Year Filter Options ---
    year_options = '<option value="all">Todos los Años</option>' + "".join([f'<option value="{y}">{y}</option>' for y in years])

    html_content = f"""
    <!DOCTYPE html>
    <html lang="es">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Dashboard de Percepción de Seguridad - Yucatán</title>
        <link rel="stylesheet" href="style.css">
        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    </head>
    <body>
        <div class="container">
            <h1>Dashboard de Percepción de Seguridad - Yucatán</h1>
            <h2>Indicadores Clave (Periodo: {latest_period})</h2>
            <div class="kpi-container">
                <div class="kpi-card"><h3>Inseguridad Estatal</h3><p>{overall_insecurity_pct:.2f}%</p></div>
                <div class="kpi-card"><h3>Municipio con Mayor Inseguridad</h3><p>{highest_insecurity_row['NOM_MUN']}</p></div>
                <div class="kpi-card"><h3>Municipio con Menor Inseguridad</h3><p>{lowest_insecurity_row['NOM_MUN']}</p></div>
            </div>
            <div class="chart-container"><h2>Ranking de Inseguridad por Municipio</h2><label for="period-selector">Selecciona un periodo:</label><select id="period-selector"></select><canvas id="rankingChart"></canvas></div>
            <div class="chart-container"><h2>Tendencia Histórica de Inseguridad Estatal</h2><img src="{TREND_IMAGE_FILE}" alt="Tendencia de Inseguridad en Yucatán"></div>
            <div class="table-container">
                <h2>Tabla de Datos Completa</h2>
                <label for="year-filter">Filtrar por Año:</label>
                <select id="year-filter">{year_options}</select>
                {table_html}
            </div>
        </div>
        <script src="dashboard_data.js"></script>
        <script src="dashboard_logic.js"></script>
    </body>
    </html>
    """
    with open(HTML_FILE, 'w', encoding='utf-8') as f:
        f.write(html_content)
    print(f"HTML file created at: {HTML_FILE}")

def main():
    """Main function to generate the dashboard."""
    print("===== Iniciando la Creación del Dashboard =====")
    try:
        df = pd.read_csv(DATA_FILE)
    except FileNotFoundError:
        print(f"Error: No se encontró el archivo de datos en {DATA_FILE}.")
        return

    generate_css()
    generate_js_files(df)
    generate_html_file(df)
    
    print("===== Creación del Dashboard Finalizada =====")

if __name__ == '__main__':
    main()
