import pandas as pd
import os
import re
import unicodedata
from logging_config import logger

# Mapeo de esquemas para años con formatos de columna diferentes
SCHEMA_MAPPINGS = {
    '2015': {'sec': 'P1', 'ent': 'ENT', 'mun': 'CD'},
    '2016': {'sec': 'bp1_1', 'ent': 'ENT', 'mun': 'MUN'}
}

def normalize_text(text):
    if not isinstance(text, str):
        return text
    text = unicodedata.normalize('NFD', text)
    text = ''.join(c for c in text if unicodedata.category(c) != 'Mn')
    return text.upper()

def get_year_quarter_from_path(path):
    path_lower = path.lower()
    match = re.search(r'ensu_(\d{4})_(\d)t_csv', path_lower)
    if match: return match.group(1), match.group(2)
    match = re.search(r'ensu_(\d{2})_(\d{4})', path_lower)
    if match: return match.group(2), str(int(match.group(1)))
    match = re.search(r'ensu_cb_(\d{2})(\d{2})', path_lower)
    if match: return f"20{match.group(2)}", str(int(match.group(1)) // 3)
    match = re.search(r'_(\d{4})_', path_lower)
    if match: return match.group(1), "DESCONOCIDO"
    match = re.search(r'(\d{4})', path_lower)
    if match: return match.group(1), "DESCONOCIDO"
    return "DESCONOCIDO", "DESCONOCIDO"

def process_survey_data(file_path, output_dir):
    logger.info(f"Iniciando procesamiento para: {os.path.basename(file_path)}")
    
    year, quarter = get_year_quarter_from_path(file_path)
    is_legacy = year in SCHEMA_MAPPINGS

    output_filename = f"resultados_ensu_{year}_{quarter}.csv"
    output_path = os.path.join(output_dir, output_filename)

    try:
        if is_legacy:
            schema = SCHEMA_MAPPINGS[year]
            required_cols = [schema['ent'], schema['mun'], schema['sec']]
            rename_map = {schema['ent']: 'NOM_ENT', schema['mun']: 'NOM_MUN', schema['sec']: 'BP1_1'}
        else:
            required_cols = ['NOM_ENT', 'NOM_MUN', 'BP1_1']
            rename_map = {}

        df = pd.read_csv(file_path, encoding='utf-8-sig', low_memory=False)

        if not all(col in df.columns for col in required_cols):
            logger.error(f"Archivo omitido: {os.path.basename(file_path)} no contiene las columnas requeridas ({required_cols}).")
            return

        if rename_map:
            df.rename(columns=rename_map, inplace=True)

        df['NOM_ENT'] = df['NOM_ENT'].astype(str).apply(normalize_text if not is_legacy else lambda x: x)
        df['NOM_MUN'] = df['NOM_MUN'].astype(str)
        df['BP1_1_numeric'] = pd.to_numeric(df['BP1_1'], errors='coerce')

        valid_bp1_1 = [1.0, 2.0, 9.0]
        anomalous_rows = df[~df['BP1_1_numeric'].isin(valid_bp1_1) & df['BP1_1_numeric'].notna()]
        for index, row in anomalous_rows.iterrows():
            logger.warning(f"Valor anómalo en {os.path.basename(file_path)} [fila {index+2}]: BP1_1 es '{row['BP1_1']}'. Se excluirá.")

        agg_funcs = {
            'TOTAL_REGISTROS': ('BP1_1_numeric', 'count'),
            'TOTAL_SEGUROS': ('BP1_1_numeric', lambda x: x.eq(1).sum()),
            'TOTAL_INSEGUROS': ('BP1_1_numeric', lambda x: x.eq(2).sum()),
            'TOTAL_NO_RESPONDE': ('BP1_1_numeric', lambda x: x.eq(9).sum())
        }
        
        processed_df = df.groupby(['NOM_ENT', 'NOM_MUN']).agg(**agg_funcs).reset_index()

        processed_df['PCT_SEGUROS'] = (processed_df['TOTAL_SEGUROS'] / processed_df['TOTAL_REGISTROS'] * 100).fillna(0)
        processed_df['PCT_INSEGUROS'] = (processed_df['TOTAL_INSEGUROS'] / processed_df['TOTAL_REGISTROS'] * 100).fillna(0)
        processed_df['PCT_NO_RESPONDE'] = (processed_df['TOTAL_NO_RESPONDE'] / processed_df['TOTAL_REGISTROS'] * 100).fillna(0)
        
        processed_df['AÑO'] = year
        processed_df['TRIMESTRE'] = quarter

        if is_legacy:
            processed_df.rename(columns={'NOM_ENT': 'ENT_CODE', 'NOM_MUN': 'MUN_CODE'}, inplace=True)
            final_columns_order = ['AÑO', 'TRIMESTRE', 'ENT_CODE', 'MUN_CODE']
        else:
            final_columns_order = ['AÑO', 'TRIMESTRE', 'NOM_ENT', 'NOM_MUN']

        final_columns_order.extend([
            'TOTAL_REGISTROS', 'TOTAL_SEGUROS', 'TOTAL_INSEGUROS', 'TOTAL_NO_RESPONDE',
            'PCT_SEGUROS', 'PCT_INSEGUROS', 'PCT_NO_RESPONDE'
        ])
        processed_df = processed_df[final_columns_order]

        processed_df.to_csv(output_path, index=False)
        logger.info(f"-> Resultado guardado exitosamente en: {output_path}")

    except Exception as e:
        logger.critical(f"Error CRÍTICO al procesar {os.path.basename(file_path)}: {e}", exc_info=True)