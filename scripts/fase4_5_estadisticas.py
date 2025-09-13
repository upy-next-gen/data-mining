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
OUTPUT_JSON = os.path.join(REPORTS_DIR, 'estadisticas_finales.json')
LOG_FILE = os.path.join(BASE_DIR, 'logs', f"fase4_5_estadisticas_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

# --- Setup Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, mode='w'),
        logging.StreamHandler()
    ]
)

def main():
    logging.info("--- Starting Phase 4.5: Final Statistics Script ---")

    # Create reports directory if it doesn't exist
    os.makedirs(REPORTS_DIR, exist_ok=True)

    # 1. Find and combine all processed CSVs
    csv_files = glob.glob(os.path.join(PROCESSED_DIR, '*.csv'))
    if not csv_files:
        logging.error(f"No processed CSV files found in {PROCESSED_DIR}. Please run Phase 4 first.")
        return

    logging.info(f"Found {len(csv_files)} processed CSV files to analyze.")

    try:
        df = pd.concat((pd.read_csv(f) for f in csv_files), ignore_index=True)
        logging.info(f"Successfully combined all files into a single DataFrame with {len(df)} rows.")
    except Exception as e:
        logging.error(f"Failed to read or combine CSV files. Error: {e}", exc_info=True)
        return

    # --- 2. Calculate Global Stats ---
    df['year'] = df['PERIODO'].str.split('_').str[0].astype(int)
    
    total_registros = len(df)
    años_cubiertos = sorted(df['year'].unique().tolist())
    trimestres_totales = df['PERIODO'].nunique()
    municipios_unicos = df['NOM_MUN'].nunique()
    ciudades_unicas = df['NOM_CD'].nunique()
    total_respuestas_seguro = int(df['TOTAL_SEGUROS'].sum())
    total_respuestas_inseguro = int(df['TOTAL_INSEGUROS'].sum())
    total_no_responde = int(df['TOTAL_NO_RESPONDE'].sum())
    promedio_pct_seguros = round(df['PORCENTAJE_SEGUROS'].mean(), 2)
    promedio_pct_inseguros = round(df['PORCENTAJE_INSEGUROS'].mean(), 2)

    estadisticas_globales = {
        "total_registros": total_registros,
        "años_cubiertos": años_cubiertos,
        "trimestres_totales": trimestres_totales,
        "municipios_unicos": municipios_unicos,
        "ciudades_unicas": ciudades_unicas,
        "total_respuestas_seguro": total_respuestas_seguro,
        "total_respuestas_inseguro": total_respuestas_inseguro,
        "total_no_responde": total_no_responde,
        "promedio_pct_seguros": promedio_pct_seguros,
        "promedio_pct_inseguros": promedio_pct_inseguros,
    }
    logging.info("Successfully calculated global statistics.")

    # --- 3. Calculate Per-Year Stats ---
    estadisticas_por_año = []
    grouped_by_year = df.groupby('year')

    for year, group in grouped_by_year:
        year_stats = {
            "año": int(year),
            "trimestres": int(group['PERIODO'].nunique()),
            "registros": len(group),
            "promedio_pct_seguros": round(group['PORCENTAJE_SEGUROS'].mean(), 2),
            "promedio_pct_inseguros": round(group['PORCENTAJE_INSEGUROS'].mean(), 2)
        }
        estadisticas_por_año.append(year_stats)
    
    estadisticas_globales["estadisticas_por_año"] = estadisticas_por_año
    logging.info("Successfully calculated per-year statistics.")

    # --- 4. Assemble Final JSON ---
    final_report = {
        "fecha_generacion": datetime.now().isoformat(),
        "estadisticas_globales": estadisticas_globales
    }

    # --- 5. Write Output ---
    try:
        with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
            json.dump(final_report, f, indent=2, ensure_ascii=False)
        logging.info(f"Successfully created final statistics report: {OUTPUT_JSON}")
    except Exception as e:
        logging.error(f"Failed to write final statistics report. Error: {e}")

    logging.info("--- Statistics script finished ---")

if __name__ == "__main__":
    main()
