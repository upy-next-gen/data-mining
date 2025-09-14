import os
import json
import logging
import logging.handlers
import pandas as pd
from datetime import datetime
import unicodedata
from fase2_validacion import normalize_column_names


# ==============================
# Configuración de logs
# ==============================
def setup_logging():
    log_dir = 'logs'
    os.makedirs(log_dir, exist_ok=True)

    log_filename = os.path.join(log_dir, "fase4_procesamiento.log")

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.handlers.TimedRotatingFileHandler(
                log_filename,
                when='midnight',
                interval=1,
                backupCount=7,
                encoding="utf-8-sig"
            ),
            logging.StreamHandler()
        ]
    )


# ==============================
# Funciones utilitarias
# ==============================
def remove_accents(text: str) -> str:
    """Quita acentos/diacríticos robustamente."""
    if isinstance(text, str):
        return ''.join(
            c for c in unicodedata.normalize('NFD', text)
            if unicodedata.category(c) != 'Mn'
        )
    return text


def normalize_all_text_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Quita acentos y pone MAYÚSCULAS en todas las columnas de texto."""
    for col in df.columns:
        if pd.api.types.is_string_dtype(df[col]) or df[col].dtype == "object":
            df[col] = df[col].astype(str).map(remove_accents).str.upper()
    return df


def normalize_column_names_ascii(df: pd.DataFrame) -> pd.DataFrame:
    """Convierte nombres de columnas a ASCII/mayúsculas (AÑO -> ANIO, espacios -> _)."""
    new_cols = []
    for c in df.columns:
        c2 = remove_accents(str(c)).upper().replace(' ', '_')
        if c2 == 'AÑO':
            c2 = 'ANIO'
        new_cols.append(c2)
    df.columns = new_cols
    return df


def _read_csv_safely(path: str) -> pd.DataFrame:
    """Lee CSV probando latin1 y luego utf-8-sig."""
    try:
        return pd.read_csv(path, encoding='latin1', low_memory=False)
    except UnicodeDecodeError:
        return pd.read_csv(path, encoding='utf-8-sig', low_memory=False)


def _normalize_nom_cd_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Asegura la existencia y calidad de NOM_CD:
    - Si no existe, la crea con 'SIN_CIUDAD'.
    - Si existe:
        * quita acentos, strip y pasa a MAYÚSCULAS,
        * vacíos/nulos -> 'SIN_CIUDAD',
        * valores solo numéricos -> 'SIN_CIUDAD'.
    """
    if 'NOM_CD' not in df.columns:
        df['NOM_CD'] = 'SIN_CIUDAD'
        return df

    s = df['NOM_CD'].astype(str).map(remove_accents).str.strip().str.upper()
    s = s.replace({'': 'SIN_CIUDAD'})
    # considerar también valores como 'N/A', 'NA', '.' como vacíos opcionalmente:
    s = s.replace({'N/A': 'SIN_CIUDAD', 'NA': 'SIN_CIUDAD', '.': 'SIN_CIUDAD'})
    # números puros -> SIN_CIUDAD
    s = s.apply(lambda x: 'SIN_CIUDAD' if x.isdigit() else x)

    df['NOM_CD'] = s
    return df


# ==============================
# Proceso principal
# ==============================
def process_files():
    logging.info("Starting Phase 4: Processing")

    try:
        with open('temp/archivos_pendientes.json', 'r', encoding='utf-8') as f:
            pending_files_data = json.load(f)
        with open('temp/mapeo_archivos.json', 'r', encoding='utf-8') as f:
            mapped_files_data = json.load(f)
    except FileNotFoundError as e:
        logging.error(f"{e.filename} not found. Please run previous phases first.")
        return

    archivos_pendientes = pending_files_data.get('lista_pendientes', [])
    periodo_lookup = {item['filepath']: item.get('periodo_str', 'unknown')
                      for item in mapped_files_data.get('archivos', [])}

    all_results = []

    for filepath in archivos_pendientes:
        logging.info(f"Processing file: {filepath}")
        try:
            df = _read_csv_safely(filepath)
            df = normalize_column_names(df)

            # Normaliza columnas clave (estados/municipios/ciudades)
            for col in ['NOM_ENT', 'NOM_MUN']:
                if col in df.columns:
                    df[col] = df[col].astype(str).map(remove_accents).str.strip().str.upper()

            # NOM_CD: asegurar presencia y valor legible
            df = _normalize_nom_cd_column(df)

            # Filtrar Yucatán
            df_yucatan = df[df['NOM_ENT'] == 'YUCATAN'].copy()
            if df_yucatan.empty:
                logging.warning(f"No data for YUCATAN found in {filepath}")
                continue

            # Asegurar BP1_1 numérico
            df_yucatan['BP1_1'] = pd.to_numeric(df_yucatan['BP1_1'], errors='coerce')

            # ---- Agrupar y agregar ----
            grouped = df_yucatan.groupby(['NOM_ENT', 'NOM_MUN', 'NOM_CD']).agg(
                TOTAL_SEGUROS=('BP1_1', lambda x: (x == 1).sum()),
                TOTAL_INSEGUROS=('BP1_1', lambda x: (x == 2).sum()),
                TOTAL_NO_RESPONDE=('BP1_1', lambda x: (x == 9).sum())
            ).reset_index()

            grouped['TOTAL_RESPUESTAS'] = (
                grouped['TOTAL_SEGUROS'] +
                grouped['TOTAL_INSEGUROS'] +
                grouped['TOTAL_NO_RESPONDE']
            )

            # Evitar división por cero
            denom = grouped['TOTAL_RESPUESTAS'].replace(0, pd.NA)
            grouped['PORCENTAJE_SEGUROS'] = (grouped['TOTAL_SEGUROS'] / denom * 100).fillna(0).round(2)
            grouped['PORCENTAJE_INSEGUROS'] = (grouped['TOTAL_INSEGUROS'] / denom * 100).fillna(0).round(2)
            grouped['PORCENTAJE_NO_RESPONDE'] = (grouped['TOTAL_NO_RESPONDE'] / denom * 100).fillna(0).round(2)

            # ---- Metadatos ----
            periodo_str = periodo_lookup.get(filepath, 'unknown')
            grouped['PERIODO'] = periodo_str
            grouped['FECHA_PROCESAMIENTO'] = datetime.now().isoformat()

            grouped['AÑO'] = pd.to_numeric(grouped['PERIODO'].str.extract(r'(\d{4})')[0], errors='coerce').astype('Int64')
            grouped['TRIMESTRE'] = pd.to_numeric(grouped['PERIODO'].str.extract(r'T(\d)')[0], errors='coerce').astype('Int64')

            grouped = grouped.drop(columns=['PERIODO'])

            all_results.append(grouped)

        except Exception as e:
            logging.exception(f"Failed to process file {filepath}: {e}")

    if not all_results:
        logging.warning("No data to process. Exiting.")
        return

    final_df = pd.concat(all_results, ignore_index=True)

    # ---- Normalizar nombres de columnas y texto de TODAS las columnas de texto ----
    final_df = normalize_column_names_ascii(final_df)
    final_df = normalize_all_text_columns(final_df)

    # ---- Carpeta salida ----
    output_dir = 'data/yucatan_processed'
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, 'yucatan_security_consolidado_new.csv')

    # ---- Renombrar columnas finales ----
    final_df = final_df.rename(columns={
        'PORCENTAJE_SEGUROS': 'PCT_SEGUROS',
        'PORCENTAJE_INSEGUROS': 'PCT_INSEGUROS',
        'PORCENTAJE_NO_RESPONDE': 'PCT_NO_RESPONDE',
        'AÑO': 'ANIO'  # por si quedó con tilde antes del normalize de columnas
    })

    # ---- Guardar CSV con UTF-8 BOM (seguro para Excel y VS Code) ----
    final_df.to_csv(output_path, index=False, encoding='utf-8-sig')
    logging.info(f"Consolidated data saved to {output_path}")

    # ---- Guardar resumen ----
    summary_data = {
        "timestamp": datetime.now().isoformat(),
        "files_processed": len(archivos_pendientes),
        "total_records_processed": len(final_df),
        "output_filepath": output_path
    }
    with open('temp/procesamiento_resultados.json', 'w', encoding='utf-8') as f:
        json.dump(summary_data, f, indent=4, ensure_ascii=False)

    logging.info("Phase 4: Processing complete. Summary saved to temp/procesamiento_resultados.json")


# ==============================
# Main
# ==============================
if __name__ == "__main__":
    setup_logging()
    process_files()
