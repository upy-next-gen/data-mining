import logging
import json
import os
import re
import math
from pathlib import Path
from datetime import datetime
import pandas as pd
from collections import Counter

# --- Configuration ---
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
LOGS_DIR = PROJECT_ROOT / "logs"
TEMP_DIR = PROJECT_ROOT / "temp"
OUTPUT_JSON = TEMP_DIR / "mapeo_archivos.json"
EXCLUDE_DIRS = {'__MACOSX', 'Datos abiertos'}

def setup_logging():
    """Configures logging to both console and a file."""
    LOGS_DIR.mkdir(exist_ok=True)
    log_filename = f"fase1_mapeo_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    log_filepath = LOGS_DIR / log_filename

    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filepath, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    logging.info(f"Logging initialized. Log file: {log_filepath}")

def get_quarter_from_month(month: int) -> int:
    """Calculates the quarter (1-4) from a given month (1-12)."""
    if not 1 <= month <= 12:
        raise ValueError("Month must be between 1 and 12")
    return (month - 1) // 3 + 1

def extract_period_from_filename(filename: str) -> tuple[int | None, int | None]:
    """
    Extracts year and quarter from a filename using a series of ordered, general date patterns.
    """
    # Pattern: YYYY_Qt (e.g., 2019_2t)
    match = re.search(r'(\d{4})_(\d)t', filename, re.IGNORECASE)
    if match:
        return int(match.group(1)), int(match.group(2))

    # Pattern: _MM_YYYY (e.g., _03_2016)
    match = re.search(r'_(\d{2})_(\d{4})', filename)
    if match:
        month, year = int(match.group(1)), int(match.group(2))
        return year, get_quarter_from_month(month)

    # Pattern: _MMYY (e.g., _0322)
    match = re.search(r'_(\d{2})(\d{2})', filename)
    if match:
        month, year_short = int(match.group(1)), int(match.group(2))
        return 2000 + year_short, get_quarter_from_month(month)

    # Pattern: YYYYTQ (e.g., 2019T2)
    match = re.search(r'(\d{4})[Tt](\d)', filename)
    if match:
        return int(match.group(1)), int(match.group(2))

    return None, None

def extract_period_from_csv(filepath: Path) -> tuple[int | None, int | None]:
    """
    Extracts year and quarter from CSV content as a fallback.
    """
    period_cols = ['PER', 'PERIODO', 'periodo', 'per']
    try:
        df = pd.read_csv(filepath, nrows=5, encoding='latin1', on_bad_lines='skip', sep=',')
        for col in period_cols:
            if col in df.columns:
                period_val_series = df[col].dropna()
                if not period_val_series.empty:
                    value = str(period_val_series.iloc[0]).zfill(4)
                    if re.fullmatch(r'\d{4}', value):
                        month, year_short = int(value[:2]), int(value[2:])
                        year = 2000 + year_short
                        quarter = get_quarter_from_month(month)
                        logging.info(f"Used content fallback: Found period '{value}' in column '{col}' for file {filepath.name}")
                        return year, quarter
        return None, None
    except Exception as e:
        logging.warning(f"Could not read period from content of {filepath.name}. Error: {e}")
        return None, None

def scan_directory(base_path: Path) -> tuple[list[dict], int]:
    """
    Scans for main 'CB' CSV files inside 'conjunto_de_datos' folders.
    """
    logging.info(f"Starting recursive scan of directory: {base_path}")
    all_metadata = []
    unknown_counter = 0
    
    cb_pattern = re.compile(r'(_cb_|conjunto_de_datos_cb|ensu_cb)', re.IGNORECASE)

    all_csv_files = list(base_path.rglob('*.csv'))
    total_csv_count = len(all_csv_files)
    logging.info(f"Found {total_csv_count} total CSV files in the directory tree.")

    filtered_files = [
        file for file in all_csv_files
        if not any(part in EXCLUDE_DIRS for part in file.parts)
    ]
    logging.info(f"Processing {len(filtered_files)} files after excluding directories: {', '.join(EXCLUDE_DIRS)}")

    for file in filtered_files:
        if cb_pattern.search(file.name) and file.parent.name == 'conjunto_de_datos':
            logging.info(f"Found matching file in correct subfolder: {file.name}")
            try:
                stats = file.stat()
                
                year, trimestre = extract_period_from_filename(file.name)
                method = "filename"

                if year is None:
                    year, trimestre = extract_period_from_csv(file)
                    method = "contenido"

                if year is None:
                    method = "no_identificado"
                    unknown_counter += 1
                    periodo_str = f"UNKNOWN_{unknown_counter:03d}"
                else:
                    periodo_str = f"{year}_Q{trimestre}"

                try:
                    sample_df = pd.read_csv(file, nrows=5, encoding='latin1', on_bad_lines='skip', sep=',')
                    columnas = list(sample_df.columns)
                    total_columnas = len(columnas)
                    columnas_muestra = columnas[:10]
                except Exception:
                    columnas_muestra = []
                    total_columnas = 0

                metadata = {
                    "filepath": str(file.relative_to(PROJECT_ROOT)).replace('\\', '/'),
                    "filename": file.name,
                    "size_mb": round(stats.st_size / (1024 * 1024), 2),
                    "modified_date": datetime.fromtimestamp(stats.st_mtime).isoformat(),
                    "year": year,
                    "trimestre": trimestre,
                    "periodo_str": periodo_str,
                    "identificacion_metodo": method,
                    "columnas_muestra": columnas_muestra,
                    "total_columnas": total_columnas,
                }
                all_metadata.append(metadata)

            except Exception as e:
                logging.error(f"Failed to process file {file.name}. Error: {e}", exc_info=True)

    logging.info(f"Scan complete. Detected {len(all_metadata)} matching CB files before de-duplication.")
    return all_metadata, total_csv_count

def deduplicate_metadata(metadata_list: list[dict]) -> list[dict]:
    """
    Deduplicates metadata based on 'periodo_str', keeping the largest file.
    """
    logging.info("Starting de-duplication process...")
    grouped_by_period = {}
    for meta in metadata_list:
        period = meta['periodo_str']
        if period not in grouped_by_period:
            grouped_by_period[period] = []
        grouped_by_period[period].append(meta)

    final_list = []
    for period, items in grouped_by_period.items():
        if period.startswith('UNKNOWN'):
            final_list.extend(items)
            continue
        
        if len(items) > 1:
            best_item = sorted(items, key=lambda x: x['size_mb'], reverse=True)[0]
            final_list.append(best_item)
            logging.info(f"Period '{period}' had {len(items)} files. Selected '{best_item['filename']}' as the primary.")
        else:
            final_list.append(items[0])
            
    logging.info(f"De-duplication complete. Final file count: {len(final_list)}")
    return final_list

def save_mapping(all_metadata: list[dict], filepath: Path):
    """Saves the collected metadata to a JSON file."""
    TEMP_DIR.mkdir(exist_ok=True)
    logging.info(f"Saving metadata mapping to {filepath}")
    output_data = {
        "timestamp": datetime.now().isoformat(),
        "total_archivos": len(all_metadata),
        "archivos": all_metadata
    }
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)
        logging.info("Successfully saved JSON output.")
    except Exception as e:
        logging.error(f"Failed to write JSON file to {filepath}. Error: {e}")

def main():
    """Main execution function."""
    setup_logging()
    logging.info("--- Fase 1: Mapeo y Descubrimiento (v7 - Final) ---")
    
    all_metadata, total_csv_count = scan_directory(DATA_DIR)

    if not all_metadata:
        logging.warning("No matching CB files were found. Exiting.")
        return

    final_metadata = deduplicate_metadata(all_metadata)
    final_metadata.sort(key=lambda x: x['periodo_str'])

    logging.info("--- Resumen del Mapeo Final ---")
    logging.info(f"Total de archivos CSV en el árbol: {total_csv_count}")
    logging.info(f"Total de archivos CB únicos procesados: {len(final_metadata)}")
    identified_count = sum(1 for m in final_metadata if m['identificacion_metodo'] != 'no_identificado')
    unidentified_count = len(final_metadata) - identified_count
    logging.info(f"Con período identificado: {identified_count}")
    logging.info(f"Sin período identificado: {unidentified_count}")
    
    period_counts = Counter(m['periodo_str'] for m in final_metadata)
    logging.info("--- Detalle por Período (únicos) ---")
    for period, count in sorted(period_counts.items()):
        logging.info(f"- {period}: {count} archivo(s)")

    save_mapping(final_metadata, OUTPUT_JSON)
    logging.info("--- Fase 1 completada ---")

if __name__ == "__main__":
    main()
