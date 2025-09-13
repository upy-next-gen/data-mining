
import pandas as pd
import glob
import os
import logging

def setup_logging():
    """Configura el logging para este script."""
    log_file = 'calculo_promedios.log'
    if logging.getLogger().hasHandlers():
        logging.getLogger().handlers.clear()

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, mode='w', encoding='utf-8'),
            logging.StreamHandler()
        ]
    )

def main():
    """Función principal para calcular promedios por dataset y generar un reporte."""
    setup_logging()
    
    input_dir = os.path.join('data', 'reporte_yucatan')
    output_file = 'Reporte_Promedio_Inseguridad_Yucatan.md'

    search_pattern = os.path.join(input_dir, 'procesado_*_yucatan.csv')
    all_files = glob.glob(search_pattern)
    
    if not all_files:
        logging.error(f"No se encontraron archivos en {input_dir}. No se puede generar el reporte.")
        return

    logging.info(f"Analizando {len(all_files)} archivos de Yucatán para calcular promedios.")
    
    average_data = []

    for filepath in all_files:
        try:
            df = pd.read_csv(filepath)
            
            if df.empty:
                logging.warning(f"El archivo {os.path.basename(filepath)} está vacío. Saltando.")
                continue

            year = df['AÑO'].iloc[0]
            quarter = df['TRIMESTRE'].iloc[0]
            
            # Cálculo del promedio ponderado
            total_inseguros = df['TOTAL_INSEGUROS'].sum()
            total_registros = df['TOTAL_REGISTROS'].sum()
            
            if total_registros == 0:
                logging.warning(f"El archivo {os.path.basename(filepath)} no tiene registros totales. Saltando.")
                continue
                
            promedio_inseguridad = round((total_inseguros / total_registros) * 100, 2)
            
            average_data.append({
                'Período': f"{year}-Q{quarter}",
                'Promedio de Inseguridad (%)': promedio_inseguridad
            })
        except Exception as e:
            logging.error(f"Error al procesar el archivo {os.path.basename(filepath)}: {e}")

    if not average_data:
        logging.error("No se pudieron calcular promedios para ningún archivo.")
        return

    # Crear DataFrame y guardarlo como Markdown
    report_df = pd.DataFrame(average_data)
    report_df.sort_values(by='Período', inplace=True)

    logging.info(f"Generando reporte en {output_file}...")
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("# Reporte de Percepción de Inseguridad Promedio por Período en Yucatán\n\n")
        f.write("Cada fila representa el promedio ponderado de la percepción de inseguridad en todos los municipios de Yucatán para el trimestre especificado.\n\n")
        f.write(report_df.to_markdown(index=False))

    logging.info("Reporte generado exitosamente.")

if __name__ == '__main__':
    main()
