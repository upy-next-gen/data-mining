
import os
import json
import logging
import glob
from datetime import datetime
import pandas as pd

# --- Configuration ---
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
REPORTS_DIR = os.path.join(PROJECT_ROOT, 'reports')
LOG_DIR = os.path.join(PROJECT_ROOT, 'logs')
PROCESSED_DIR = os.path.join(PROJECT_ROOT, 'data', 'yucatan_processed')
TEMP_DIR = os.path.join(PROJECT_ROOT, 'temp')

# --- Setup ---
def setup_environment():
    """Create necessary directories and configure logging."""
    os.makedirs(REPORTS_DIR, exist_ok=True)
    os.makedirs(LOG_DIR, exist_ok=True)

    log_filename = f"fase5_reporte_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    log_filepath = os.path.join(LOG_DIR, log_filename)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filepath),
            logging.StreamHandler()
        ]
    )
    logging.info("--- Iniciando Fase 5: Reporte Final ---")
    return logging.getLogger()

# --- Data Loading ---
def load_and_consolidate_data(logger):
    """Loads all processed CSVs into a single DataFrame."""
    csv_pattern = os.path.join(PROCESSED_DIR, 'yucatan_security_*.csv')
    csv_files = glob.glob(csv_pattern)
    
    if not csv_files:
        logger.error(f"No se encontraron archivos procesados en {PROCESSED_DIR}. Ejecute la Fase 4 primero.")
        return None

    logger.info(f"Encontrados {len(csv_files)} archivos CSV para consolidar.")
    
    try:
        df_list = [pd.read_csv(f) for f in csv_files]
        master_df = pd.concat(df_list, ignore_index=True)
        logger.info(f"Datos consolidados exitosamente. Total de filas: {len(master_df)}")
        return master_df
    except Exception as e:
        logger.error(f"Error al consolidar archivos CSV: {e}")
        return None

def load_processing_summary(logger):
    """Loads the processing summary JSON, with a fallback."""
    primary_path = os.path.join(TEMP_DIR, 'procesamiento_resultados.json')
    fallback_path = os.path.join(TEMP_DIR, 'resumen_procesamiento.json')
    summary = None
    try:
        if os.path.exists(primary_path):
            with open(primary_path, 'r', encoding='utf-8') as f:
                summary = json.load(f)
            logger.info(f"Resumen de procesamiento cargado desde {primary_path}")
        elif os.path.exists(fallback_path):
            with open(fallback_path, 'r', encoding='utf-8') as f:
                summary = json.load(f)
            logger.info(f"Resumen de procesamiento cargado desde fallback {fallback_path}")
        else:
            logger.warning("No se encontró ningún archivo de resumen de procesamiento.")
            summary = {"archivos_procesados": 0, "total_archivos": 0, "resultados": []}
    except Exception as e:
        logger.error(f"Error cargando el resumen de procesamiento: {e}")
        summary = {"archivos_procesados": 0, "total_archivos": 0, "resultados": []}
    return summary

# --- Data Processing and Statistics ---
def process_dataframe(df):
    """Normalize and create derived columns."""
    df[['AÑO', 'TRIMESTRE']] = df['PERIODO'].str.split('-Q', expand=True)
    df['AÑO'] = pd.to_numeric(df['AÑO'])
    df['TRIMESTRE'] = pd.to_numeric(df['TRIMESTRE'])

    df['PCT_SEGUROS'] = df['PORCENTAJE_SEGUROS']
    df['PCT_INSEGUROS'] = df['PORCENTAJE_INSEGUROS']
    df['PCT_NO_RESPONDE'] = df['PORCENTAJE_NO_RESPONDE']
    df['TOTAL_REGISTROS'] = df['TOTAL_RESPUESTAS']
    return df

def compute_global_stats(df):
    """Compute global and aggregated statistics from the master DataFrame."""
    if df.empty:
        return {}
    
    total_respuestas = df['TOTAL_RESPUESTAS'].sum()
    
    # Stats by Year
    stats_by_year = df.groupby('AÑO').apply(lambda x: pd.Series({
        'trimestres': x['TRIMESTRE'].nunique(),
        'registros': x['TOTAL_REGISTROS'].sum(),
        'promedio_pct_seguros': (x['TOTAL_SEGUROS'].sum() / x['TOTAL_RESPUESTAS'].sum() * 100) if x['TOTAL_RESPUESTAS'].sum() > 0 else 0,
        'promedio_pct_inseguros': (x['TOTAL_INSEGUROS'].sum() / x['TOTAL_RESPUESTAS'].sum() * 100) if x['TOTAL_RESPUESTAS'].sum() > 0 else 0
    })).reset_index()

    # Most/Least Secure Municipality
    mun_stats = df.groupby('NOM_MUN').apply(lambda x: pd.Series({
        'pct_promedio_seguros': (x['TOTAL_SEGUROS'].sum() / x['TOTAL_RESPUESTAS'].sum() * 100) if x['TOTAL_RESPUESTAS'].sum() > 0 else 0,
        'pct_promedio_inseguros': (x['TOTAL_INSEGUROS'].sum() / x['TOTAL_RESPUESTAS'].sum() * 100) if x['TOTAL_RESPUESTAS'].sum() > 0 else 0
    })).reset_index()

    municipio_mas_seguro = mun_stats.loc[mun_stats['pct_promedio_seguros'].idxmax()]
    municipio_mas_inseguro = mun_stats.loc[mun_stats['pct_promedio_inseguros'].idxmax()]

    stats = {
        'total_registros': int(df['TOTAL_REGISTROS'].sum()),
        'años_cubiertos': sorted(df['AÑO'].unique().tolist()),
        'trimestres_totales': int(df['PERIODO'].nunique()),
        'municipios_unicos': int(df['NOM_MUN'].nunique()),
        'ciudades_unicas': int(df['NOM_CD'].nunique()),
        'total_respuestas_seguro': int(df['TOTAL_SEGUROS'].sum()),
        'total_respuestas_inseguro': int(df['TOTAL_INSEGUROS'].sum()),
        'total_no_responde': int(df['TOTAL_NO_RESPONDE'].sum()),
        'promedio_pct_seguros': round((df['TOTAL_SEGUROS'].sum() / total_respuestas * 100) if total_respuestas > 0 else 0, 2),
        'promedio_pct_inseguros': round((df['TOTAL_INSEGUROS'].sum() / total_respuestas * 100) if total_respuestas > 0 else 0, 2),
        'estadisticas_por_año': stats_by_year.to_dict('records'),
        'municipio_mas_seguro': municipio_mas_seguro.to_dict(),
        'municipio_mas_inseguro': municipio_mas_inseguro.to_dict()
    }
    return stats

# --- HTML Report Generation ---
def generate_html_report(stats, summary, logger):
    """Generates a self-contained HTML report."""
    logger.info("Generando reporte HTML...")
    now = datetime.now()
    
    # Helper for safe dictionary access
    def get_stat(key, default='N/A'):
        return stats.get(key, default)

    # Build table rows dynamically
    years_table_rows = ""
    for row in get_stat('estadisticas_por_año', []):
        years_table_rows += f"""<tr>
            <td>{row.get('AÑO', 'N/A')}</td>
            <td>{row.get('trimestres', 'N/A')}</td>
            <td>{row.get('registros', 'N/A'):,}</td>
            <td>{row.get('promedio_pct_seguros', 0):.2f}%</td>
            <td>{row.get('promedio_pct_inseguros', 0):.2f}%</td>
        </tr>"""

    files_table_rows = ""
    if summary.get('resultados'):
        for res in summary['resultados']:
            status_class = 'exito' if res.get('exito') else 'fallido'
            status_text = 'Éxito' if res.get('exito') else 'Fallido'
            error_msg = res.get('error', '-')
            files_table_rows += f"""<tr>
                <td>{res.get('periodo', 'N/A')}</td>
                <td>{os.path.basename(res.get('archivo_origen', ''))}</td>
                <td class='{status_class}'>{status_text}</td>
                <td>{error_msg}</td>
            </tr>"""
        files_section = f"""<h2>Detalle de Archivos Procesados</h2><table>
            <thead><tr><th>Periodo</th><th>Archivo Origen</th><th>Estado</th><th>Error (si aplica)</th></tr></thead>
            <tbody>{files_table_rows}</tbody>
        </table>"""
    else:
        files_section = "<p>No se encontró el resumen detallado del procesamiento.</p>"

    html_template = f"""<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <title>Reporte de Percepción de Seguridad - Yucatán</title>
    <style>
        body {{ font-family: sans-serif; margin: 20px; background-color: #f4f4f9; color: #333; }}
        .container {{ max-width: 1200px; margin: auto; background: #fff; padding: 20px; border-radius: 8px; box-shadow: 0 0 10px rgba(0,0,0,0.1); }}
        h1, h2 {{ color: #444; border-bottom: 2px solid #005a9c; padding-bottom: 10px; }}
        .summary-cards {{ display: flex; flex-wrap: wrap; gap: 20px; margin-bottom: 20px; }}
        .card {{ background: #eaf4ff; border-left: 5px solid #005a9c; padding: 20px; border-radius: 5px; flex-grow: 1; text-align: center; }}
        .card h3 {{ margin-top: 0; color: #005a9c; }} .card p {{ font-size: 24px; margin: 0; font-weight: bold; }}
        .ranking {{ display: flex; gap: 20px; }}
        .rank-item {{ flex-basis: 50%; background: #f0f0f0; padding: 15px; border-radius: 5px; }}
        table {{ width: 100%; border-collapse: collapse; margin-top: 20px; }} th, td {{ padding: 12px; border: 1px solid #ddd; text-align: left; }} 
        thead {{ background-color: #005a9c; color: white; }} tbody tr:nth-child(even) {{ background-color: #f2f2f2; }}
        .exito {{ color: green; }} .fallido {{ color: red; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>Reporte de Percepción de Seguridad en Yucatán</h1>
        <p>Fecha de generación: {now.strftime('%Y-%m-%d %H:%M:%S')}</p>
        
        <h2>Resumen General</h2>
        <div class="summary-cards">
            <div class="card"><h3>Archivos Procesados</h3><p>{summary.get('archivos_procesados', 0)} / {summary.get('total_archivos', 0)}</p></div>
            <div class="card"><h3>Total Registros Analizados</h3><p>{get_stat('total_registros', 0):,}</p></div>
            <div class="card"><h3>Años Cubiertos</h3><p>{get_stat('años_cubiertos', [])[0]} - {get_stat('años_cubiertos', [])[-1]}</p></div>
            <div class="card"><h3>Trimestres Únicos</h3><p>{get_stat('trimestres_totales', 0)}</p></div>
        </div>

        <h2>Promedios Globales de Percepción</h2>
        <div class="summary-cards">
            <div class="card"><h3>% Seguro</h3><p>{get_stat('promedio_pct_seguros', 0):.2f}%</p></div>
            <div class="card"><h3>% Inseguro</h3><p>{get_stat('promedio_pct_inseguros', 0):.2f}%</p></div>
        </div>

        <h2>Estadísticas por Año</h2>
        <table><thead><tr><th>Año</th><th>Trimestres</th><th>Registros</th><th>Promedio % Seguro</th><th>Promedio % Inseguro</th></tr></thead><tbody>{years_table_rows}</tbody></table>

        <h2>Rankings de Municipios</h2>
        <div class="ranking">
            <div class="rank-item"><h3>Municipio con Mayor Percepción de Seguridad</h3><p>{get_stat('municipio_mas_seguro', {}).get('NOM_MUN', 'N/A')} ({get_stat('municipio_mas_seguro', {}).get('pct_promedio_seguros', 0):.2f}%)</p></div>
            <div class="rank-item"><h3>Municipio con Mayor Percepción de Inseguridad</h3><p>{get_stat('municipio_mas_inseguro', {}).get('NOM_MUN', 'N/A')} ({get_stat('municipio_mas_inseguro', {}).get('pct_promedio_inseguros', 0):.2f}%)</p></div>
        </div>

        {files_section}
    </div>
</body>
</html>"""
    return html_template

# --- Main Logic ---
def main():
    logger = setup_environment()
    
    master_df = load_and_consolidate_data(logger)
    if master_df is None or master_df.empty:
        logger.error("No hay datos para procesar. Finalizando.")
        return

    master_df = process_dataframe(master_df)
    
    # 5. Save consolidated CSV
    master_csv_path = os.path.join(REPORTS_DIR, 'dataset_maestro_yucatan.csv')
    master_df.to_csv(master_csv_path, index=False, encoding='utf-8')
    logger.info(f"Dataset maestro guardado en: {master_csv_path}")

    # 6. Compute global stats
    global_stats = compute_global_stats(master_df)
    
    # 7. Load processing summary
    processing_summary = load_processing_summary(logger)

    # 8. Generate HTML report
    html_content = generate_html_report(global_stats, processing_summary, logger)
    html_report_path = os.path.join(REPORTS_DIR, 'reporte_ensu_yucatan.html')
    with open(html_report_path, 'w', encoding='utf-8') as f:
        f.write(html_content)
    logger.info(f"Reporte HTML guardado en: {html_report_path}")

    # 9. Save final stats JSON
    final_stats_output = {
        "fecha_generacion": datetime.now().isoformat(),
        "estadisticas_globales": global_stats,
        "resumen_procesamiento": processing_summary
    }
    stats_json_path = os.path.join(REPORTS_DIR, 'estadisticas_finales.json')
    with open(stats_json_path, 'w', encoding='utf-8') as f:
        json.dump(final_stats_output, f, indent=4, ensure_ascii=False)
    logger.info(f"Estadísticas finales guardadas en: {stats_json_path}")

    logger.info("--- Fase 5 Finalizada ---")

if __name__ == "__main__":
    main()
