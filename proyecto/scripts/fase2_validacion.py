import json
import logging
from datetime import datetime
from pathlib import Path
import pandas as pd

# --- Configuration ---
PROJECT_ROOT = Path(__file__).resolve().parent.parent
TEMP_DIR = PROJECT_ROOT / "temp"
LOG_DIR = PROJECT_ROOT / "logs"

INPUT_JSON = TEMP_DIR / "mapeo_archivos.json"
OUTPUT_JSON = TEMP_DIR / "archivos_validados.json"

REQUIRED_COLUMNS = {'NOM_ENT', 'NOM_MUN', 'BP1_1'}
VALID_BP1_1_VALUES = {1, 2, 9}

# --- Setup ---
def setup_environment():
    """Create necessary directories and configure logging."""
    TEMP_DIR.mkdir(exist_ok=True)
    LOG_DIR.mkdir(exist_ok=True)

    log_filename = f"fase2_validacion_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    log_filepath = LOG_DIR / log_filename

    # Clear existing handlers
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
    logging.info("--- Iniciando Fase 2: Validación de Archivos ---")
    return logging.getLogger()

# --- Validation Functions ---
def validate_columns(filepath: Path, logger: logging.Logger):
    """Checks for the presence of required columns."""
    try:
        # Using utf-8-sig to handle potential BOM
        df_sample = pd.read_csv(filepath, nrows=5, encoding='utf-8-sig', low_memory=False, sep=',')
        present_columns = set(df_sample.columns)
        missing_columns = list(REQUIRED_COLUMNS - present_columns)
        return {
            "columnas_presentes": not bool(missing_columns),
            "columnas_faltantes": missing_columns,
            "columnas_totales": len(present_columns)
        }
    except Exception as e:
        logger.error(f"Validación de columnas falló para {filepath.name}: {e}")
        return {"columnas_presentes": False, "columnas_faltantes": list(REQUIRED_COLUMNS), "columnas_totales": 0}

def validate_bp1_1(filepath: Path, logger: logging.Logger):
    """Validates the values within the BP1_1 column."""
    result = {"valores_validos": False, "valores_unicos": [], "valores_invalidos": [], "distribucion": {}, "nulls_count": 0, "nulls_percentage": 1.0, "muestra_size": 0}
    try:
        df = pd.read_csv(filepath, usecols=['BP1_1'], nrows=1000, encoding='latin1', low_memory=False)
        result['muestra_size'] = int(len(df))
        if df.empty:
            return result

        series = pd.to_numeric(df['BP1_1'], errors='coerce')
        coerced_nulls = series.isnull().sum()
        result['nulls_count'] = int(coerced_nulls)
        if not df.empty:
            result['nulls_percentage'] = round(float(coerced_nulls) / len(df), 4)

        series.dropna(inplace=True)
        series = series.astype(int)
        
        unique_values = set(series.unique())
        invalid_values = list(unique_values - VALID_BP1_1_VALUES)
        
        result.update({
            "valores_validos": not bool(invalid_values),
            "valores_unicos": sorted([int(v) for v in unique_values]),
            "valores_invalidos": sorted([int(v) for v in invalid_values]),
            "distribucion": {str(k): int(v) for k, v in series.value_counts().items()}
        })
        return result
    except ValueError:
        logger.warning(f"Columna BP1_1 no encontrada en {filepath.name}.")
        return result
    except Exception as e:
        logger.error(f"Validación de BP1_1 falló para {filepath.name}: {e}")
        return result

def validate_yucatan(filepath: Path, logger: logging.Logger):
    """Checks for the presence and percentage of 'YUCATAN' records."""
    result = {"tiene_yucatan": False, "registros_yucatan": 0, "total_registros": 0, "porcentaje_yucatan": 0.0, "estados_unicos": 0}
    try:
        df = pd.read_csv(filepath, usecols=['NOM_ENT'], encoding='latin1', low_memory=False)
        total_registros = int(len(df))
        result['total_registros'] = total_registros
        if df.empty:
            return result

        df.dropna(subset=['NOM_ENT'], inplace=True)
        df_norm = df['NOM_ENT'].str.strip().str.upper()
        
        yucatan_count = int(df_norm.str.contains('YUCAT', na=False).sum())
        
        result.update({
            "tiene_yucatan": bool(yucatan_count > 0),
            "registros_yucatan": yucatan_count,
            "porcentaje_yucatan": round(yucatan_count / total_registros if total_registros > 0 else 0, 4),
            "estados_unicos": int(df_norm.nunique())
        })
        return result
    except ValueError:
        logger.warning(f"Columna NOM_ENT no encontrada en {filepath.name}.")
        return result
    except Exception as e:
        logger.error(f"Validación de Yucatán falló para {filepath.name}: {e}")
        return result

# --- Main Logic ---
def main():
    logger = setup_environment()

    try:
        with open(INPUT_JSON, 'r', encoding='utf-8') as f:
            mapped_data = json.load(f)
        logger.info(f"Cargado exitosamente {INPUT_JSON}")
    except FileNotFoundError:
        logger.error(f"Error: No se encontró el archivo de entrada {INPUT_JSON}. Ejecute la Fase 1 primero.")
        return

    all_validations = []
    for file_info in mapped_data['archivos']:
        filepath_relative = Path(file_info['filepath'])
        filepath_absolute = PROJECT_ROOT / filepath_relative
        logger.info(f"Validando archivo: {filepath_absolute.name}")
        
        val_cols = validate_columns(filepath_absolute, logger)
        val_bp1_1 = validate_bp1_1(filepath_absolute, logger) if val_cols['columnas_presentes'] else {}
        val_yuc = validate_yucatan(filepath_absolute, logger) if val_cols['columnas_presentes'] else {}

        is_processable = (
            val_cols.get('columnas_presentes', False) and
            val_bp1_1.get('valores_validos', False) and
            val_yuc.get('tiene_yucatan', False)
        )

        razon = ""
        if not val_cols.get('columnas_presentes', False):
            razon = f"Columnas requeridas faltantes: {val_cols.get('columnas_faltantes')}"
        elif not val_bp1_1.get('valores_validos', False):
            razon = f"Valores inválidos en BP1_1: {val_bp1_1.get('valores_invalidos')}"
        elif not val_yuc.get('tiene_yucatan', False):
            razon = "No se encontraron registros de Yucatán."

        all_validations.append({
            "filepath": str(filepath_relative), # Keep relative path in output
            "periodo_str": file_info['periodo_str'],
            "modified_date": file_info['modified_date'],
            "validacion_columnas": val_cols,
            "validacion_bp1_1": val_bp1_1,
            "validacion_yucatan": val_yuc,
            "es_procesable": is_processable,
            "razon_no_procesable": razon
        })

    # --- Duplicate Resolution ---
    procesable_by_period = {}
    for val in all_validations:
        if val['es_procesable']:
            period = val['periodo_str']
            if period not in procesable_by_period:
                procesable_by_period[period] = []
            procesable_by_period[period].append(val)

    archivos_seleccionados = {}
    duplicados_info = {}

    for period, candidates in procesable_by_period.items():
        if len(candidates) > 1:
            logger.info(f"Resolviendo duplicados para el periodo {period}")
            candidates.sort(key=lambda x: (x['validacion_yucatan'].get('registros_yucatan', 0), x['modified_date']), reverse=True)
            
            winner = candidates[0]
            losers = candidates[1:]
            archivos_seleccionados[period] = winner['filepath']
            duplicados_info[period] = {
                "archivo_seleccionado": winner['filepath'],
                "criterio": f"Registros Yucatán: {winner['validacion_yucatan'].get('registros_yucatan', 0)}, Fecha Mod: {winner['modified_date']}",
                "descartados": [l['filepath'] for l in losers]
            }

            for loser in losers:
                for original_val in all_validations:
                    if original_val['filepath'] == loser['filepath']:
                        original_val['es_procesable'] = False
                        original_val['razon_no_procesable'] = f"Duplicado - seleccionado: {Path(winner['filepath']).name}"
                        break
        elif candidates:
            archivos_seleccionados[period] = candidates[0]['filepath']

    procesables_count = sum(1 for v in all_validations if v['es_procesable'])
    no_procesables_count = len(all_validations) - procesables_count

    for val in all_validations:
        val.pop('modified_date', None)

    # --- Final Output ---
    final_output = {
        "timestamp": datetime.now().isoformat(),
        "total_archivos_validados": len(all_validations),
        "procesables": procesables_count,
        "no_procesables": no_procesables_count,
        "duplicados_info": duplicados_info,
        "archivos_seleccionados_procesables": archivos_seleccionados,
        "validaciones": all_validations
    }

    try:
        with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
            json.dump(final_output, f, indent=4, ensure_ascii=False)
        logger.info(f"Resultados de validación guardados en: {OUTPUT_JSON}")
    except Exception as e:
        logger.error(f"No se pudo escribir el archivo JSON de salida: {e}")

    logger.info(f"--- Fase 2 Finalizada: {procesables_count} procesables, {no_procesables_count} no procesables ---")

if __name__ == "__main__":
    main()