
import os
import glob
import logging
import re
import pandas as pd
from procesar_datos import procesar_datos

def get_year_quarter_from_path(file_path):
    año, trimestre = 'NA', 'NA'
    # Patrón para YYYY_Qt (e.g., ..._2018_1t_...)
    match = re.search(r'_(\d{4})_(\d)t_', file_path)
    if match:
        año, trimestre = match.groups()

    # Patrón para MM_YYYY (e.g., ..._01_2016_...)
    if año == 'NA':
        match = re.search(r'_(\d{2})_(\d{4})', file_path)
        if match:
            mes, año = int(match.group(1)), match.group(2)
            trimestre = (mes - 1) // 3 + 1

    # Patrón para MMYY (e.g., ..._cb_0322.csv)
    if año == 'NA':
        match = re.search(r'_cb_(\d{2})(\d{2})\.csv', file_path)
        if match:
            mes, anio_corto = int(match.group(1)), match.group(2)
            año = f"20{anio_corto}"
            trimestre = (mes - 1) // 3 + 1
    
    return str(año), str(trimestre)

def main():
    log_file = 'procesamiento.log'
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, mode='w'),
            logging.StreamHandler()
        ]
    )

    logging.info("Starting data mapping and processing.")

    output_dir = os.path.join('Data', 'yucatan-inseguridad')
    os.makedirs(output_dir, exist_ok=True)
    logging.info(f"Output directory ensured: {output_dir}")

    data_files = glob.glob('Data/**/conjunto_de_datos*cb*.csv', recursive=True)
    data_files = [f for f in data_files if '__MACOSX' not in f]
    
    logging.info(f"Found {len(data_files)} data files to process.")

    # Group files by year and quarter
    grouped_files = {}
    for file_path in data_files:
        year, quarter = get_year_quarter_from_path(file_path)
        if year != 'NA':
            if (year, quarter) not in grouped_files:
                grouped_files[(year, quarter)] = []
            grouped_files[(year, quarter)].append(file_path)

    logging.info(f"Grouped files into {len(grouped_files)} year/quarter buckets.")

    for (year, quarter), files in grouped_files.items():
        logging.info(f"Processing {len(files)} files for Year: {year}, Quarter: {quarter}")
        
        # Read and concatenate all dataframes for the group
        df_list = []
        for file_path in files:
            try:
                df = pd.read_csv(file_path, encoding='utf-8', low_memory=False)
                df_list.append(df)
            except UnicodeDecodeError:
                logging.warning(f"UTF-8 decoding error in {file_path}, trying latin1.")
                try:
                    df = pd.read_csv(file_path, encoding='latin1', low_memory=False)
                    df_list.append(df)
                except Exception as e:
                    logging.error(f"Could not read file {file_path}. Error: {e}")
        
        if not df_list:
            logging.error(f"No dataframes could be created for Year: {year}, Quarter: {quarter}. Skipping.")
            continue

        full_df = pd.concat(df_list, ignore_index=True)
        procesar_datos(full_df, output_dir, year, quarter)

    logging.info("Data mapping and processing finished.")

if __name__ == "__main__":
    main()
