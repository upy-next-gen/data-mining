
import pandas as pd
import glob
import os
import re
import matplotlib.pyplot as plt

def generate_timeseries_report():
    """
    Agrega los datos de seguridad de Yucatán, genera una gráfica de evolución
    y compila un reporte en formato Markdown.
    """
    # Definición de rutas
    PROCESSED_FILES_PATH = "data/processed/processed_ensu_*.csv"
    REPORTS_DIR = "reports"
    IMAGES_DIR = os.path.join(REPORTS_DIR, "images")
    PLOT_FILENAME = os.path.join(IMAGES_DIR, "percepcion_seguridad_yucatan_evolucion.png")
    REPORT_FILENAME = os.path.join(REPORTS_DIR, "reporte_analisis_temporal.md")

    # 1. Asegurar que el directorio de imágenes exista
    os.makedirs(IMAGES_DIR, exist_ok=True)
    print(f"Directorio de imágenes asegurado en: {IMAGES_DIR}")

    # 2. Agregar datos de todos los archivos
    results = []
    file_list = sorted(glob.glob(PROCESSED_FILES_PATH))

    for file_path in file_list:
        match = re.search(r'_(\d{4})_q(\d)', file_path)
        if not match:
            continue
        
        year, quarter = int(match.group(1)), int(match.group(2))
        
        try:
            df = pd.read_csv(file_path)
            if df.empty:
                continue

            df['BP1_1'] = pd.to_numeric(df['BP1_1'], errors='coerce')
            
            total_respuestas = len(df)
            total_seguros = (df['BP1_1'] == 1).sum()
            total_inseguros = (df['BP1_1'] == 2).sum()
            
            pct_seguros = (total_seguros / total_respuestas * 100) if total_respuestas > 0 else 0
            pct_inseguros = (total_inseguros / total_respuestas * 100) if total_respuestas > 0 else 0

            results.append({
                'Año': year,
                'Trimestre': quarter,
                'Periodo': f'{year}-T{quarter}',
                '% Seguros': round(pct_seguros, 2),
                '% Inseguros': round(pct_inseguros, 2),
                'Total Respuestas': total_respuestas
            })
        except Exception as e:
            print(f"No se pudo procesar {file_path}: {e}")

    if not results:
        print("No se pudieron agregar datos. No se puede generar el reporte.")
        return

    time_series_df = pd.DataFrame(results)
    print("Datos agregados exitosamente.")

    # 3. Generar la gráfica
    plt.style.use('seaborn-v0_8-whitegrid')
    fig, ax = plt.subplots(figsize=(14, 8))
    
    ax.plot(time_series_df['Periodo'], time_series_df['% Inseguros'], marker='o', linestyle='-', color='#c0392b', label='% Inseguros')
    ax.plot(time_series_df['Periodo'], time_series_df['% Seguros'], marker='o', linestyle='-', color='#27ae60', label='% Seguros')

    ax.set_title('Evolución de la Percepción de Seguridad en Yucatán', fontsize=18, fontweight='bold', pad=20)
    ax.set_xlabel('Periodo (Año-Trimestre)', fontsize=12)
    ax.set_ylabel('Porcentaje de Respuestas (%)', fontsize=12)
    ax.legend()
    ax.grid(True, which='both', linestyle='--', linewidth=0.5)
    
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig(PLOT_FILENAME)
    print(f"Gráfica guardada en: {PLOT_FILENAME}")

    # 4. Generar el reporte en Markdown
    max_inseguridad = time_series_df.loc[time_series_df['% Inseguros'].idxmax()]
    min_inseguridad = time_series_df.loc[time_series_df['% Inseguros'].idxmin()]
    avg_inseguridad = time_series_df['% Inseguros'].mean()

    # Usar una ruta relativa para la imagen en el MD
    relative_plot_path = os.path.relpath(PLOT_FILENAME, REPORTS_DIR)

    md_content = f"""# Análisis Temporal de la Percepción de Seguridad en Yucatán

## Introducción

Este reporte presenta un análisis de la evolución de la percepción de seguridad en Yucatán, basado en los datos trimestrales de la Encuesta Nacional de Seguridad Pública Urbana (ENSU) que han sido procesados y filtrados.

## Evolución Gráfica de la Percepción de Seguridad

La siguiente gráfica muestra la tendencia de los porcentajes de percepción de seguridad e inseguridad a lo largo del tiempo.

![Evolución de la Percepción de Seguridad]({relative_plot_path})

## Análisis de Tendencias

- **Punto más alto de inseguridad:** La percepción de inseguridad alcanzó su pico en el periodo **{max_inseguridad['Periodo']}** con un **{max_inseguridad['% Inseguros']:.2f}%**.
- **Punto más bajo de inseguridad:** El momento con la menor percepción de inseguridad fue en **{min_inseguridad['Periodo']}** con un **{min_inseguridad['% Inseguros']:.2f}%**.
- **Promedio:** El promedio de percepción de inseguridad a lo largo de todos los periodos analizados es de **{avg_inseguridad:.2f}%**.

## Resumen de Datos por Periodo

A continuación se presenta la tabla con los datos detallados para cada trimestre.

"""

    md_content += time_series_df.to_markdown(index=False)

    with open(REPORT_FILENAME, 'w', encoding='utf-8') as f:
        f.write(md_content)
    
    print(f"Reporte guardado en: {REPORT_FILENAME}")

if __name__ == "__main__":
    generate_timeseries_report()
