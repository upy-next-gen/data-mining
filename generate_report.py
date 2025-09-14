
import pandas as pd
import os
import glob
import logging
import matplotlib.pyplot as plt
import base64
from io import BytesIO

# --- Configuration ---
BASE_DIR = r"C:\Users\elian\downloads\Inseguridad_yucatan"
DATA_DIR = os.path.join(BASE_DIR, "data", "yucatan-inseguridad")
OUTPUT_HTML = os.path.join(BASE_DIR, "reporte_yucatan_evolucion.html")
LOG_FILE = os.path.join(BASE_DIR, "log_analisis_yucatan.log")

# --- Setup Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, mode='w', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# --- Main Functions ---
def consolidate_data():
    """Finds and consolidates all processed CSVs into a single DataFrame."""
    logging.info(f"Searching for processed files in: {DATA_DIR}")
    csv_files = glob.glob(os.path.join(DATA_DIR, "procesado_*.csv"))
    if not csv_files:
        logging.error("No processed CSV files found. Aborting.")
        return None
    
    logging.info(f"Found {len(csv_files)} files to consolidate.")
    df_list = [pd.read_csv(f) for f in csv_files]
    df = pd.concat(df_list, ignore_index=True)
    logging.info(f"Consolidated DataFrame shape: {df.shape}")
    
    # Create a proper time period column for sorting and plotting
    df['PERIODO'] = pd.to_datetime(df['AÑO'].astype(str) + 'Q' + df['TRIMESTRE'].astype(str))
    df = df.sort_values('PERIODO').reset_index(drop=True)
    logging.info("Created and sorted by 'PERIODO' column.")
    return df

def calculate_state_average(df):
    """Calculates the statewide weighted average for PCT_INSEGUROS."""
    logging.info("Calculating statewide weighted average...")
    df_weighted = df.copy()
    df_weighted['WEIGHTED_PCT'] = df_weighted['PCT_INSEGUROS'] * df_weighted['TOTAL_REGISTROS']
    
    state_avg = df_weighted.groupby('PERIODO').agg(
        SUM_WEIGHTED_PCT=('WEIGHTED_PCT', 'sum'),
        SUM_TOTAL_REGISTROS=('TOTAL_REGISTROS', 'sum')
    ).reset_index()
    
    state_avg['PCT_INSEGUROS_ESTATAL'] = (state_avg['SUM_WEIGHTED_PCT'] / state_avg['SUM_TOTAL_REGISTROS']).round(2)
    logging.info("Statewide weighted average calculation complete.")
    return state_avg[['PERIODO', 'PCT_INSEGUROS_ESTATAL']]

def plot_to_base64(fig):
    """Converts a Matplotlib figure to a base64 encoded string."""
    buf = BytesIO()
    fig.savefig(buf, format='png', bbox_inches='tight')
    plt.close(fig)
    return base64.b64encode(buf.getvalue()).decode('utf-8')

def create_plot(df, y_column, title, y_label, is_state=False):
    """Creates a time series plot and returns it as a base64 string."""
    logging.info(f"Creating plot: {title}")
    fig, ax = plt.subplots(figsize=(10, 5))
    
    # For handling gaps, create a full date range
    if not df.empty:
        full_range = pd.date_range(start=df['PERIODO'].min(), end=df['PERIODO'].max(), freq='QS-OCT')
        df_plot = df.set_index('PERIODO').reindex(full_range).reset_index().rename(columns={'index': 'PERIODO'})
    else:
        df_plot = df

    ax.plot(df_plot['PERIODO'], df_plot[y_column], marker='o', linestyle='-')
    ax.set_title(title)
    ax.set_ylabel(y_label)
    ax.set_xlabel("Periodo")
    ax.grid(True, which='both', linestyle='--', linewidth=0.5)
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    return plot_to_base64(fig)

def generate_html_report(state_plot, mun_plots, consolidated_df):
    logging.info("Generating HTML report...")
    
    # --- HTML Styling ---
    html_style = """
    <style>
        body { font-family: sans-serif; margin: 2em; background-color: #f9f9f9; color: #333; }
        h1, h2 { color: #1a237e; border-bottom: 2px solid #3949ab; padding-bottom: 5px; }
        .container { background-color: #fff; padding: 2em; border-radius: 8px; box-shadow: 0 4px 8px rgba(0,0,0,0.1); }
        .plot { text-align: center; margin-bottom: 3em; }
        img { max-width: 100%; height: auto; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        table { border-collapse: collapse; width: 100%; margin-top: 2em; }
        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
        th { background-color: #3949ab; color: white; }
        tr:nth-child(even) { background-color: #f2f2f2; }
        .notes { background-color: #e8eaf6; border-left: 5px solid #3949ab; padding: 1em; margin-top: 2em; border-radius: 5px; }
    </style>
    """
    
    # --- HTML Content ---
    municipal_plots_html = "".join([f'<div class="plot"><h2>{name}</h2><img src="data:image/png;base64,{plot_b64}" alt="Gráfico de {name}"></div>' for name, plot_b64 in mun_plots.items()])
    
    html_content = f"""
    <!DOCTYPE html>
    <html lang="es">
    <head>
        <meta charset="UTF-8">
        <title>Reporte de Percepción de Inseguridad en Yucatán</title>
        {html_style}
    </head>
    <body>
        <div class="container">
            <h1>Reporte de Evolución de Percepción de Inseguridad en Yucatán</h1>
            
            <div class="plot">
                <h2>Evolución Estatal (Promedio Ponderado)</h2>
                <img src="data:image/png;base64,{state_plot}" alt="Gráfico estatal">
            </div>
            
            <hr>
            
            {municipal_plots_html}
            
            <hr>

            <h2>Datos Consolidados</h2>
            {consolidated_df.to_html(index=False, classes='data_table')}

            <div class="notes">
                <h2>Notas Metodológicas</h2>
                <p><strong>Fuente de Datos:</strong> Encuesta Nacional de Seguridad Pública Urbana (ENSU) del INEGI, a partir de los datos procesados.</p>
                <p><strong>Procesamiento:</strong> Los datos de múltiples trimestres fueron consolidados. Se identificaron y procesaron tanto formatos de archivo modernos (posteriores a 2018) como antiguos.</p>
                <p><strong>Promedio Estatal:</strong> El promedio estatal de percepción de inseguridad (PCT_INSEGUROS) se calcula como un promedio ponderado. El factor de ponderación es el total de registros (encuestas) por municipio (`TOTAL_REGISTROS`) para cada trimestre. Esto asegura que los municipios con más encuestas tengan un peso proporcionalmente mayor en el promedio estatal.</p>
                <p><strong>Manejo de Datos Faltantes:</strong> En los gráficos, los periodos sin datos disponibles (gaps) se muestran como interrupciones en las líneas, representando visualmente la ausencia de información para ese trimestre específico.</p>
            </div>
        </div>
    </body>
    </html>
    """

    with open(OUTPUT_HTML, 'w', encoding='utf-8') as f:
        f.write(html_content)
    logging.info(f"HTML report successfully generated at: {OUTPUT_HTML}")

# --- Main Execution ---
if __name__ == '__main__':
    logging.info("=== Report Generation Script Start ===")
    
    df = consolidate_data()
    
    if df is not None:
        state_avg_df = calculate_state_average(df)
        
        # Generate state plot
        state_plot_b64 = create_plot(state_avg_df, 'PCT_INSEGUROS_ESTATAL', 'Percepción de Inseguridad en Yucatán (Estatal)', 'Porcentaje de Inseguridad (%)', is_state=True)
        
        # Generate municipal plots
        municipal_plots = {}
        for municipality in df['NOM_MUN'].unique():
            df_mun = df[df['NOM_MUN'] == municipality]
            plot_b64 = create_plot(df_mun, 'PCT_INSEGUROS', f'Percepción de Inseguridad en {municipality}', 'Porcentaje de Inseguridad (%)')
            municipal_plots[municipality] = plot_b64
            
        # Generate HTML report
        generate_html_report(state_plot_b64, municipal_plots, df)
    
    logging.info("=== Report Generation Script End ===")
