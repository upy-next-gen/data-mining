import os
import re
import json
import logging
import logging.handlers
import pandas as pd
from datetime import datetime

def setup_logging():
    """Sets up logging to a file in the logs directory."""
    log_dir = 'logs'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    log_filename = os.path.join(log_dir, "fase1_mapeo.log")
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.handlers.TimedRotatingFileHandler(
                log_filename,
                when='midnight',
                interval=1,
                backupCount=7
            ),
            logging.StreamHandler()
        ]
    )

def extract_periodo_info(path):
    """Extracts year, quarter, and other period info from the file path using regex."""
    # Try to find YYYYTQ pattern first, e.g., 2022T1
    match = re.search(r'(\d{4})T(\d)', path)
    if match:
        year, trimestre = map(int, match.groups())
        return year, trimestre, f"{year}T{trimestre}", "YYYYTQ"

    # Try to find YYYY_Qt pattern, e.g., 2019_2t
    match = re.search(r'(\d{4})_(\d)t', path, re.IGNORECASE)
    if match:
        year, trimestre = map(int, match.groups())
        return year, trimestre, f"{year}T{trimestre}", "YYYY_Qt"

    # Try to find MM_YYYY pattern, e.g., 01_2015
    match = re.search(r'_(\d{2})_(\d{4})', path, re.IGNORECASE)
    if match:
        month, year = map(int, match.groups())
        trimestre = (month - 1) // 3 + 1
        return year, trimestre, f"{year}T{trimestre}", "MM_YYYY"
        
    # Try to find MMYY pattern, e.g., 0118
    match = re.search(r'_(\d{2})(\d{2})', path, re.IGNORECASE)
    if match:
        month, year_short = map(int, match.groups())
        year = 2000 + year_short
        trimestre = (month - 1) // 3 + 1
        return year, trimestre, f"{year}T{trimestre}", "cb_MMYY"

    return None, None, None, None

def extract_metadata(file_path):
    """Extracts metadata from a given file path."""
    try:
        # Basic file info
        size_mb = os.path.getsize(file_path) / (1024 * 1024)
        modified_date = datetime.fromtimestamp(os.path.getmtime(file_path)).isoformat()
        filename = os.path.basename(file_path)

        # CSV-specific info
        try:
            df = pd.read_csv(file_path, encoding='latin1', low_memory=False)
            total_columnas = len(df.columns)
            columnas_muestra = df.columns.tolist()[:5]
        except Exception as e:
            logging.warning(f"Could not read CSV {file_path}: {e}")
            total_columnas = None
            columnas_muestra = None

        # Metadata from path
        year, trimestre, periodo_str, identificacion_metodo = extract_periodo_info(file_path)

        return {
            "filepath": file_path,
            "filename": filename,
            "size_mb": round(size_mb, 4),
            "modified_date": modified_date,
            "year": year,
            "trimestre": trimestre,
            "periodo_str": periodo_str,
            "identificacion_metodo": identificacion_metodo,
            "columnas_muestra": columnas_muestra,
            "total_columnas": total_columnas,
        }
    except Exception as e:
        logging.error(f"Error processing file {file_path}: {e}")
        return None

def map_files():
    """Maps all ENSU files in the data directory and extracts metadata."""
    logging.info("Starting Phase 1: File Mapping and Discovery")
    
    data_dir = 'data'
    pattern = re.compile(r'ensu.*\.csv', re.IGNORECASE)
    
    archivos = []
    
    for root, _, files in os.walk(data_dir):
        for file in files:
            if pattern.search(file):
                file_path = os.path.join(root, file)
                logging.info(f"Found matching file: {file_path}")
                metadata = extract_metadata(file_path)
                if metadata:
                    archivos.append(metadata)

    output_dir = 'temp'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        
    output_path = os.path.join(output_dir, 'mapeo_archivos.json')
    
    output_data = {
        "timestamp": datetime.now().isoformat(),
        "total_archivos": len(archivos),
        "archivos": archivos
    }
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=4, ensure_ascii=False)
        
    logging.info(f"File mapping complete. Catalog saved to {output_path}")

if __name__ == "__main__":
    setup_logging()
    map_files()