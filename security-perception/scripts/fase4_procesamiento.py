#!/usr/bin/env python3
"""
Fase 4: Procesamiento (Versión Final con unificación de Mérida)
"""
import json
import logging
import pandas as pd
import os
from datetime import datetime
import unicodedata

def setup_logging():
    os.makedirs("logs", exist_ok=True)
    logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s',
                        handlers=[logging.FileHandler(f"logs/fase4_procesamiento_{datetime.now().strftime('%Y%m%d')}.log"),
                                  logging.StreamHandler()])

def load_pending_files(filepath="temp/archivos_pendientes.json"):
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f).get('archivos_pendientes', [])

def normalize_text(text):
    """Función para quitar acentos y convertir a mayúsculas."""
    if not isinstance(text, str):
        return text
    text = unicodedata.normalize('NFD', text).encode('ascii', 'ignore').decode('utf-8')
    return text.upper().strip()

def process_single_file(file_info):
    logger = logging.getLogger(__name__)
    filepath = file_info['filepath']
    logger.info(f"--- Procesando {os.path.basename(filepath)} ---")

    try:
        available_cols = pd.read_csv(filepath, nrows=0, encoding='utf-8').columns
        use_cols = ['BP1_1']
        
        ent_col = 'NOM_ENT' if 'NOM_ENT' in available_cols else 'CVE_ENT' if 'CVE_ENT' in available_cols else 'ENT'
        mun_col = 'NOM_MUN' if 'NOM_MUN' in available_cols else 'CVE_MUN' if 'CVE_MUN' in available_cols else None
        cd_col = 'NOM_CD' if 'NOM_CD' in available_cols else None
        
        if not ent_col:
            logger.warning(f"SALTANDO: El archivo {os.path.basename(filepath)} no contiene columna de entidad.")
            return None
            
        use_cols.extend([ent_col])
        if mun_col: use_cols.append(mun_col)
        if cd_col: use_cols.append(cd_col)
        
        df = pd.read_csv(filepath, usecols=use_cols, low_memory=False, encoding='utf-8', dtype={ent_col: str})
        
        clean_ent_col = df[ent_col].str.replace(r'[\r\n]', '', regex=True).str.strip()
        filtro_yucatan = (clean_ent_col.str.upper().str.contains('YUCAT', na=False)) | (clean_ent_col == '31')
        df_yucatan = df[filtro_yucatan].copy()

        if df_yucatan.empty:
            logger.warning(f"No se encontraron datos para Yucatán usando el filtro. Asumiendo que todo el archivo es de Yucatán.")
            df_yucatan = df.copy()

        rename_map = {ent_col: 'NOM_ENT'}
        if mun_col: rename_map[mun_col] = 'NOM_MUN'
        if cd_col: rename_map[cd_col] = 'NOM_CD'
        df_yucatan.rename(columns=rename_map, inplace=True)

        # --- INICIO DE LA CORRECCIÓN FINAL ---
        if 'NOM_MUN' not in df_yucatan.columns:
            logger.warning(f"No se encontró columna de Municipio. Asignando 'MERIDA' por defecto.")
            # Se asigna el nombre normalizado para que coincida con los otros archivos.
            df_yucatan['NOM_MUN'] = 'MERIDA'
        # --- FIN DE LA CORRECCIÓN FINAL ---

        df_yucatan['NOM_MUN'] = df_yucatan['NOM_MUN'].apply(normalize_text)
        
        if 'NOM_CD' not in df_yucatan.columns:
            df_yucatan['NOM_CD'] = 'SIN_CIUDAD'
        else:
            df_yucatan['NOM_CD'].fillna('SIN_CIUDAD', inplace=True)
        
        df_yucatan['BP1_1'] = pd.to_numeric(df_yucatan['BP1_1'], errors='coerce')
        df_yucatan.dropna(subset=['BP1_1'], inplace=True)
        df_yucatan['BP1_1'] = df_yucatan['BP1_1'].astype(int)

        grouped = df_yucatan.groupby(['NOM_MUN', 'NOM_CD'])['BP1_1'].value_counts().unstack(fill_value=0)
        
        grouped.rename(columns={1: 'TOTAL_SEGUROS', 2: 'TOTAL_INSEGUROS', 9: 'TOTAL_NO_RESPONDE'}, inplace=True)
        for col in ['TOTAL_SEGUROS', 'TOTAL_INSEGUROS', 'TOTAL_NO_RESPONDE']:
            if col not in grouped.columns: grouped[col] = 0
        
        df_resultado = grouped.reset_index()
        df_resultado['TOTAL_REGISTROS'] = df_resultado['TOTAL_SEGUROS'] + df_resultado['TOTAL_INSEGUROS'] + df_resultado['TOTAL_NO_RESPONDE']
        df_resultado['PCT_SEGUROS'] = (df_resultado['TOTAL_SEGUROS'] / df_resultado['TOTAL_REGISTROS'] * 100).round(2)
        df_resultado['PCT_INSEGUROS'] = (df_resultado['TOTAL_INSEGUROS'] / df_resultado['TOTAL_REGISTROS'] * 100).round(2)
        df_resultado['PCT_NO_RESPONDE'] = (df_resultado['TOTAL_NO_RESPONDE'] / df_resultado['TOTAL_REGISTROS'] * 100).round(2)
        df_resultado['NOM_ENT'] = 'Yucatán'
        df_resultado['AÑO'] = file_info.get('año')
        df_resultado['TRIMESTRE'] = file_info.get('trimestre')
        
        columnas_finales = ['NOM_ENT', 'NOM_MUN', 'NOM_CD', 'TOTAL_REGISTROS', 'TOTAL_SEGUROS', 'TOTAL_INSEGUROS', 'TOTAL_NO_RESPONDE', 'PCT_SEGUROS', 'PCT_INSEGUROS', 'PCT_NO_RESPONDE', 'AÑO', 'TRIMESTRE']
        return df_resultado[columnas_finales]
        
    except Exception as e:
        logger.error(f"✗ Error procesando {filepath}: {e}", exc_info=True)
        return None

def main():
    setup_logging()
    logger = logging.getLogger(__name__)
    logger.info("=== INICIANDO FASE 4 (con unificación de Mérida) ===")
    files_to_process = load_pending_files()
    output_dir = "data/yucatan_processed"
    os.makedirs(output_dir, exist_ok=True)
    for file_info in files_to_process:
        df_processed = process_single_file(file_info)
        if df_processed is not None and not df_processed.empty:
            output_path = os.path.join(output_dir, f"yucatan_security_{file_info['periodo_str']}.csv")
            df_processed.to_csv(output_path, index=False, encoding='utf-8')
            logger.info(f"Archivo procesado guardado en: {output_path}")
    logger.info("=== FASE 4 COMPLETADA ===")

if __name__ == "__main__":
    main()