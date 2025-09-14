# -*- coding: utf-8 -*-
import os
import json
import logging
import logging.handlers
from datetime import datetime

import pandas as pd


# ============================================================
# Logging
# ============================================================
def setup_logging():
    os.makedirs("logs", exist_ok=True)
    os.makedirs("temp", exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = os.path.join("logs", f"fase2_validacion_{timestamp}.log")

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Evita handlers duplicados si se re-ejecuta en el mismo proceso
    if logger.handlers:
        for h in list(logger.handlers):
            logger.removeHandler(h)

    fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)

    fh = logging.handlers.RotatingFileHandler(
        log_path, maxBytes=5_000_000, backupCount=3, encoding="utf-8"
    )
    fh.setLevel(logging.INFO)
    fh.setFormatter(fmt)

    logger.addHandler(ch)
    logger.addHandler(fh)


# ============================================================
# Utilidades de normalización
# ============================================================
def remove_accents(input_str):
    if isinstance(input_str, str):
        s = (input_str
             .replace('á', 'a').replace('é', 'e').replace('í', 'i').replace('ó', 'o').replace('ú', 'u')
             .replace('Á', 'A').replace('É', 'E').replace('Í', 'I').replace('Ó', 'O').replace('Ú', 'U')
             .replace('ñ', 'n').replace('Ñ', 'N'))
        return s
    return input_str


def _coalesce_duplicate_columns(df: pd.DataFrame, name: str) -> pd.DataFrame:
    """
    Si existen varias columnas con el mismo nombre `name`,
    combina por primer valor no-nulo (de izquierda a derecha) y elimina duplicados.
    """
    idxs = [i for i, c in enumerate(df.columns) if c == name]
    if len(idxs) <= 1:
        return df  # no hay duplicados

    # bloque con todas las columnas duplicadas
    block = df.iloc[:, idxs]
    # primer no-nulo por fila
    merged = block.bfill(axis=1).iloc[:, 0]

    # reconstruir df sin duplicados (conservando la primera posición)
    drop_pos = idxs[1:]
    keep_pos = [i for i in range(df.shape[1]) if i not in drop_pos]
    df2 = df.iloc[:, keep_pos].copy()

    first_pos = idxs[0]
    df2.iloc[:, first_pos] = merged

    return df2


def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Estandariza nombres de columnas y valores clave (ENT/MUN/CD y BP1_1),
    coalesce de duplicados y mapeo de códigos de entidad.
    """
    df = df.copy()

    # Limpia nombres de columnas
    df.columns = (df.columns.astype(str)
                  .str.replace('ï»¿', '', regex=False)
                  .str.strip()
                  .str.replace('"', '', regex=False))

    # Renombrado a estándar
    rename_map = {
        'ENT': 'NOM_ENT', 'CVE_ENT': 'NOM_ENT', 'ID_ENTIDAD': 'NOM_ENT',
        'CD': 'NOM_CD', 'CIUDAD': 'NOM_CD',
        'BP11': 'BP1_1', 'BP1_1_A': 'BP1_1',
    }
    # Handle 'MUN' column: if it's a name and NOM_MUN doesn't exist, rename it.
    if 'MUN' in df.columns and 'NOM_MUN' not in df.columns:
        if not pd.api.types.is_numeric_dtype(df['MUN']):
            df.rename(columns={'MUN': 'NOM_MUN'}, inplace=True)
        elif 'ID_MUNICIPIO' not in df.columns: # If 'MUN' is numeric and NOM_MUN doesn't exist, treat as ID
            df.rename(columns={'MUN': 'ID_MUNICIPIO'}, inplace=True)
    
    # Ensure CVE_MUN is consistently named ID_MUNICIPIO if it exists and ID_MUNICIPIO doesn't
    if 'CVE_MUN' in df.columns and 'ID_MUNICIPIO' not in df.columns:
        df.rename(columns={'CVE_MUN': 'ID_MUNICIPIO'}, inplace=True)
    df.rename(columns=rename_map, inplace=True, errors="ignore")

    # Coalescer duplicados de las columnas que nos importan
    for target in ['NOM_ENT', 'NOM_MUN', 'NOM_CD', 'BP1_1']:
        if target in df.columns:
            df = _coalesce_duplicate_columns(df, target)

    # Si quedaran duplicadas por alguna razón, dejar la primera y avisar
    if df.columns.duplicated().any():
        dups = df.columns[df.columns.duplicated()].unique().tolist()
        logging.warning(f"Columnas duplicadas tras rename: {dups}. Se conservará la primera ocurrencia.")
        df = df.loc[:, ~df.columns.duplicated(keep='first')].copy()

    # Catálogo INEGI
    state_codes_map = {
        '01': 'AGUASCALIENTES', '1': 'AGUASCALIENTES',
        '02': 'BAJA CALIFORNIA', '2': 'BAJA CALIFORNIA',
        '03': 'BAJA CALIFORNIA SUR', '3': 'BAJA CALIFORNIA SUR',
        '04': 'CAMPECHE', '4': 'CAMPECHE',
        '05': 'COAHUILA DE ZARAGOZA', '5': 'COAHUILA DE ZARAGOZA',
        '06': 'COLIMA', '6': 'COLIMA',
        '07': 'CHIAPAS', '7': 'CHIAPAS',
        '08': 'CHIHUAHUA', '8': 'CHIHUAHUA',
        '09': 'CIUDAD DE MEXICO', '9': 'CIUDAD DE MEXICO',
        '10': 'DURANGO',
        '11': 'GUANAJUATO',
        '12': 'GUERRERO',
        '13': 'HIDALGO',
        '14': 'JALISCO',
        '15': 'MEXICO',
        '16': 'MICHOACAN DE OCAMPO',
        '17': 'MORELOS',
        '18': 'NAYARIT',
        '19': 'NUEVO LEON',
        '20': 'OAXACA',
        '21': 'PUEBLA',
        '22': 'QUERETARO',
        '23': 'QUINTANA ROO',
        '24': 'SAN LUIS POTOSI',
        '25': 'SINALOA',
        '26': 'SONORA',
        '27': 'TABASCO',
        '28': 'TAMAULIPAS',
        '29': 'TLAXCALA',
        '30': 'VERACRUZ DE IGNACIO DE LA LLAVE',
        '31': 'YUCATAN',
        '32': 'ZACATECAS'
    }

    # Normalización de texto en columnas clave
    for col in ['NOM_ENT', 'NOM_MUN', 'NOM_CD']:
        if col in df.columns:
            df[col] = (df[col].astype(str).fillna('')
                        .apply(lambda x: remove_accents(str(x)).strip().upper()))

    # Mapear códigos de entidad a nombre
    if 'NOM_ENT' in df.columns:
        def map_ent(v):
            s = str(v).strip()
            if s.isdigit():
                return state_codes_map.get(s.zfill(2), state_codes_map.get(s, s))
            return s
        df['NOM_ENT'] = (df['NOM_ENT'].apply(map_ent)
                            .astype(str).fillna('')
                            .apply(lambda x: remove_accents(str(x)).strip().upper()))

    return df


# ============================================================
# Validaciones
# ============================================================
def validate_columns(df: pd.DataFrame, required_columns):
    present_columns = [col for col in required_columns if col in df.columns]
    missing_columns = [col for col in required_columns if col not in df.columns]
    return {
        "columnas_presentes": present_columns,
        "columnas_faltantes": missing_columns,
        "columnas_totales": int(len(df.columns))
    }


def validate_bp1_1(df: pd.DataFrame):
    if 'BP1_1' not in df.columns:
        return None

    valid_values = {1, 2, 9}
    df['BP1_1'] = pd.to_numeric(df['BP1_1'], errors='coerce')

    unique_values = set(df['BP1_1'].dropna().unique())
    invalid_values = sorted([int(x) for x in (unique_values - valid_values)])

    distribucion = {
        (str(int(k)) if pd.notna(k) else "NaN"): int(v)
        for k, v in df['BP1_1'].value_counts(dropna=False).items()
    }

    return {
        "valores_validos": sorted(list(valid_values)),
        "valores_unicos": sorted([int(x) for x in unique_values]),
        "valores_invalidos": invalid_values,
        "distribucion": distribucion,
        "nulls_count": int(df['BP1_1'].isnull().sum()),
        "nulls_percentage": float(df['BP1_1'].isnull().mean()),
        "muestra_size": int(len(df))
    }


def validate_yucatan(df: pd.DataFrame):
    if 'NOM_ENT' not in df.columns:
        return None

    total_records = len(df)
    yucatan_records = df[df['NOM_ENT'] == 'YUCATAN']

    return {
        "tiene_yucatan": not yucatan_records.empty,
        "registros_yucatan": int(len(yucatan_records)),
        "total_registros": int(total_records),
        "porcentaje_yucatan": float(len(yucatan_records) / total_records if total_records > 0 else 0.0),
        "estados_unicos": [str(x) for x in df['NOM_ENT'].dropna().unique().tolist()]
    }


# ============================================================
# Flujo principal
# ============================================================
def _read_csv_safely(path: str) -> pd.DataFrame:
    """
    Lee CSV probando primero latin1 (típico ENSU) y luego utf-8-sig.
    """
    try:
        return pd.read_csv(path, encoding='latin1', low_memory=False)
    except UnicodeDecodeError:
        return pd.read_csv(path, encoding='utf-8-sig', low_memory=False)


def validate_files():
    logging.info("Starting Phase 2: Validation")

    mapeo_path = os.path.join("temp", "mapeo_archivos.json")
    if not os.path.exists(mapeo_path):
        logging.error("temp/mapeo_archivos.json not found. Please run Phase 1 first.")
        return

    try:
        with open(mapeo_path, 'r', encoding='utf-8') as f:
            mapped_files = json.load(f)
    except Exception as e:
        logging.exception(f"No se pudo leer {mapeo_path}: {e}")
        return

    required_columns = ['NOM_ENT', 'NOM_MUN', 'NOM_CD', 'BP1_1']
    validations = []
    procesables_count = 0

    archivos = mapped_files.get('archivos', [])
    for file_info in archivos:
        filepath = file_info.get('filepath')
        periodo_str = file_info.get('periodo_str')
        logging.info(f"Validating file: {filepath}")

        try:
            df = _read_csv_safely(filepath)
            df = normalize_column_names(df)

            column_validation = validate_columns(df, required_columns)
            bp1_1_validation = validate_bp1_1(df)
            yucatan_validation = validate_yucatan(df)

            is_processable = (
                column_validation.get('columnas_faltantes') == [] and
                bp1_1_validation is not None and bp1_1_validation.get('valores_invalidos') == [] and
                yucatan_validation is not None and yucatan_validation.get('tiene_yucatan', False)
            )

            if is_processable:
                procesables_count += 1

            validations.append({
                "filepath": filepath,
                "periodo_str": periodo_str,
                "validacion_columnas": column_validation,
                "validacion_bp1_1": bp1_1_validation,
                "validacion_yucatan": yucatan_validation,
                "es_procesable": is_processable
            })

        except Exception as e:
            logging.exception(f"Could not process file {filepath}: {e}")
            validations.append({
                "filepath": filepath,
                "periodo_str": periodo_str,
                "error": str(e),
                "es_procesable": False
            })

    output_data = {
        "timestamp": datetime.now().isoformat(),
        "total_archivos": len(archivos),
        "procesables": procesables_count,
        "no_procesables": len(archivos) - procesables_count,
        "duplicados_info": {},
        "archivos_seleccionados": [v['filepath'] for v in validations if v.get('es_procesable')],
        "validaciones": validations
    }

    out_path = os.path.join("temp", "archivos_validados.json")
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=4, ensure_ascii=False)

    logging.info(f"Phase 2: Validation complete. Results saved to {out_path}")


# ============================================================
# Entry point
# ============================================================
if __name__ == "__main__":
    setup_logging()
    validate_files()
