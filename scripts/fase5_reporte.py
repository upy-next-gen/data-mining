import os
import json
import logging
import pandas as pd
import glob
from datetime import datetime

# --- Configuration ---
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
PROCESSED_DIR = os.path.join(BASE_DIR, 'data', 'yucatan_processed')
REPORTS_DIR = os.path.join(BASE_DIR, 'reports')
STATS_JSON = os.path.join(REPORTS_DIR, 'estadisticas_finales.json')
OUTPUT_HTML = os.path.join(REPORTS_DIR, 'dashboard.html')
LOG_FILE = os.path.join(BASE_DIR, 'logs', f"fase5_reporte_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

# --- Setup Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, mode='w'),
        logging.StreamHandler()
    ]
)

HTML_TEMPLATE = '''
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Dashboard de Percepción de Seguridad en Yucatán</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
    <link href="https://cdn.datatables.net/1.13.7/css/dataTables.bootstrap5.min.css" rel="stylesheet">
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <style>
        body {{ background-color: #f8f9fa; }}
        .card {{ margin-bottom: 1.5rem; box-shadow: 0 0.125rem 0.25rem rgba(0,0,0,.075); }}
        .chart-container {{ background-color: white; padding: 2rem; border-radius: 0.5rem; box-shadow: 0 0.125rem 0.25rem rgba(0,0,0,.075); }}
        .text-muted-custom {{ color: #6c757d; font-size: 0.9rem; margin-top: 0.5rem; }}
    </style>
</head>
<body>
    <div class="container-fluid mt-4">
        <div class="px-3 py-3 pt-md-5 pb-md-4 mx-auto text-center">
            <h1 class="display-4">Percepción de Seguridad en Yucatán</h1>
            <p class="lead">Análisis de datos históricos de la encuesta ENSU. Fecha del reporte: {fecha_generacion}</p>
        </div>

        <!-- Section 1: KPIs -->
        <div class="row text-center">
            <div class="col-md-6">
                <div class="card text-white" style="background-color: #00695C;">
                    <div class="card-header">Municipio con Mayor Inseguridad (Promedio)</div>
                    <div class="card-body">
                        <h2 class="card-title">{lugar_mas_inseguro}</h2>
                        <p class="card-text" style="font-size: 1.5rem;">{max_inseguridad_pct}%</p>
                    </div>
                </div>
            </div>
            <div class="col-md-6">
                <div class="card text-white" style="background-color: #20c997;">
                    <div class="card-header">Municipio con Menor Inseguridad (Promedio)</div>
                    <div class="card-body">
                        <h2 class="card-title">{lugar_mas_seguro}</h2>
                        <p class="card-text" style="font-size: 1.5rem;">{min_inseguridad_pct}%</p>
                    </div>
                </div>
            </div>
        </div>

        <!-- Section 2: Time-series Chart -->
        <div class="row mt-4">
            <div class="col-12">
                <div class="chart-container">
                    <h3>Evolución de la Percepción de Inseguridad (Promedio Estatal)</h3>
                    <div id="line-chart"></div>
                    <p class="text-muted-custom">Este gráfico muestra el promedio del porcentaje de percepción de inseguridad en todo el estado de Yucatán a través de los diferentes trimestres, permitiendo identificar tendencias y cambios a lo largo del tiempo.</p>
                </div>
            </div>
        </div>

        <!-- Section 3: Bar Chart -->
        <div class="row mt-4">
            <div class="col-12">
                <div class="chart-container">
                    <h3>Inseguridad Promedio por Municipio</h3>
                    <div id="bar-chart"></div>
                    <p class="text-muted-custom">Este gráfico clasifica los municipios según su percepción de inseguridad promedio, calculada a partir de todos los periodos disponibles. Permite comparar directamente qué municipios son percibidos como más o menos seguros.</p>
                </div>
            </div>
        </div>

        <!-- Section 4: Data Table -->
        <div class="row mt-4">
            <div class="col-12">
                <div class="chart-container">
                    <h3>Datos Completos</h3>
                    {table_html}
                    <p class="text-muted-custom">La siguiente tabla contiene los datos agregados utilizados para generar las visualizaciones. Puede ordenar las columnas o utilizar el buscador para explorar los datos en detalle.</p>
                </div>
            </div>
        </div>

    </div>

    <script src="https://code.jquery.com/jquery-3.7.0.js"></script>
    <script src="https://cdn.datatables.net/1.13.7/js/jquery.dataTables.min.js"></script>
    <script src="https://cdn.datatables.net/1.13.7/js/dataTables.bootstrap5.min.js"></script>

    <script>
        const TEAL_COLOR = '#20c997';

        // Data injected from Python
        const lineData = {line_data_json};
        const barData = {bar_data_json};

        // Line Chart
        const lineTrace = {{
            x: lineData.periodo,
            y: lineData.porcentaje_inseguros,
            mode: 'lines+markers',
            name: 'Inseguridad',
            marker: {{ color: TEAL_COLOR }}
        }};
        const lineLayout = {{
            xaxis: {{ title: 'Periodo' }},
            yaxis: {{ title: 'Porcentaje de Inseguridad' }}
        }};
        Plotly.newPlot('line-chart', [lineTrace], lineLayout);

        // Bar Chart
        const barTrace = {{
            x: barData.porcentaje_inseguros,
            y: barData.municipio,
            type: 'bar',
            orientation: 'h',
            marker: {{ color: TEAL_COLOR }}
        }};
        const barLayout = {{
            yaxis: {{ automargin: true }},
            xaxis: {{ title: 'Inseguridad Promedio (%)' }}
        }};
        Plotly.newPlot('bar-chart', [barTrace], barLayout);

        // Data Table
        $(document).ready(function() {{
            $('#dataTable').DataTable();
        }});
    </script>
</body>
</html>
'''

def main():
    logging.info("--- Starting Phase 5: Report Generation Script ---")

    os.makedirs(REPORTS_DIR, exist_ok=True)

    # 1. Load and combine data
    csv_files = glob.glob(os.path.join(PROCESSED_DIR, '*.csv'))
    if not csv_files:
        logging.error(f"No processed CSV files found in {PROCESSED_DIR}.")
        return
    
    df = pd.concat((pd.read_csv(f) for f in csv_files), ignore_index=True)
    df['year'] = df['PERIODO'].str.split('_').str[0].astype(int)

    # 2. Load stats for KPIs
    with open(STATS_JSON, 'r', encoding='utf-8') as f:
        stats = json.load(f)
    fecha_generacion = datetime.fromisoformat(stats['fecha_generacion']).strftime('%d-%m-%Y %H:%M')

    # 3. Calculate data for visualizations
    # Bar chart data (used for KPIs and the chart itself)
    bar_df = df.groupby('NOM_MUN')[['PORCENTAJE_INSEGUROS']].mean().sort_values(by='PORCENTAJE_INSEGUROS', ascending=False).reset_index()

    # Extremes (based on average)
    lugar_mas_inseguro_info = bar_df.iloc[0]
    lugar_mas_seguro_info = bar_df.iloc[-1]
    lugar_mas_inseguro = lugar_mas_inseguro_info['NOM_MUN']
    max_inseguridad_pct = round(lugar_mas_inseguro_info['PORCENTAJE_INSEGUROS'], 2)
    lugar_mas_seguro = lugar_mas_seguro_info['NOM_MUN']
    min_inseguridad_pct = round(lugar_mas_seguro_info['PORCENTAJE_INSEGUROS'], 2)

    # Time-series data
    line_df = df.groupby('PERIODO')[['PORCENTAJE_INSEGUROS']].mean().sort_index().reset_index()

    # Data for table
    df_for_table = df.rename(columns={
        'NOM_MUN': 'Municipio',
        'PORCENTAJE_INSEGUROS': '% Inseguridad',
        'PERIODO': 'Periodo'
    })[['Municipio', '% Inseguridad', 'Periodo']]
    table_html = df_for_table.to_html(classes='table table-striped', table_id='dataTable', index=False)

    # 4. Generate HTML
    line_data_dict = {
        'periodo': line_df['PERIODO'].tolist(),
        'porcentaje_inseguros': line_df['PORCENTAJE_INSEGUROS'].tolist()
    }
    line_data_json = json.dumps(line_data_dict)

    bar_data_dict = {
        'municipio': bar_df['NOM_MUN'].tolist(),
        'porcentaje_inseguros': bar_df['PORCENTAJE_INSEGUROS'].tolist()
    }
    bar_data_json = json.dumps(bar_data_dict)

    final_html = HTML_TEMPLATE.format(
        fecha_generacion=fecha_generacion,
        lugar_mas_inseguro=lugar_mas_inseguro,
        max_inseguridad_pct=max_inseguridad_pct,
        lugar_mas_seguro=lugar_mas_seguro,
        min_inseguridad_pct=min_inseguridad_pct,
        line_data_json=line_data_json,
        bar_data_json=bar_data_json,
        table_html=table_html
    )

    # 5. Write HTML file
    with open(OUTPUT_HTML, 'w', encoding='utf-8') as f:
        f.write(final_html)
    
    logging.info(f"Successfully created dashboard report: {OUTPUT_HTML}")
    logging.info("--- Report generation finished ---")

if __name__ == "__main__":
    main()
