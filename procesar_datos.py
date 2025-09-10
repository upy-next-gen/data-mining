import pandas as pd
import os
import logging
from verificar_datos import verificar_dataset

estados = {
    1: 'AGUASCALIENTES', 2: 'BAJA CALIFORNIA', 3: 'BAJA CALIFORNIA SUR', 4: 'CAMPECHE',
    5: 'COAHUILA DE ZARAGOZA', 6: 'COLIMA', 7: 'CHIAPAS', 8: 'CHIHUAHUA',
    9: 'CIUDAD DE MEXICO', 10: 'DURANGO', 11: 'GUANAJUATO', 12: 'GUERRERO',
    13: 'HIDALGO', 14: 'JALISCO', 15: 'MEXICO', 16: 'MICHOACAN DE OCAMPO',
    17: 'MORELOS', 18: 'NAYARIT', 19: 'NUEVO LEON', 20: 'OAXACA',
    21: 'PUEBLA', 22: 'QUERETARO', 23: 'QUINTANA ROO', 24: 'SAN LUIS POTOSI',
    25: 'SINALOA', 26: 'SONORA', 27: 'TABASCO', 28: 'TAMAULIPAS',
    29: 'TLAXCALA', 30: 'VERACRUZ DE IGNACIO DE LA LLAVE', 31: 'YUCATAN', 32: 'ZACATECAS'
}

def normalizar_texto(texto):
    if not isinstance(texto, str):
        return texto
    texto = texto.strip()
    texto = texto.upper()
    texto = texto.replace('Ñ', 'N')
    return texto

def procesar_datos(df, output_dir, year, quarter):
    logging.info(f"Processing data for Year: {year}, Quarter: {quarter}")

    column_mapping = {
        'ENT': 'NOM_ENT',
        'NOM_ENT': 'NOM_ENT',
        'MUN': 'NOM_MUN',
        'NOM_MUN': 'NOM_MUN',
        'BP1_1': 'BP1_1'
    }
    df.rename(columns=lambda c: column_mapping.get(c.strip(), c.strip()), inplace=True)

    if 'NOM_ENT' in df.columns and pd.api.types.is_numeric_dtype(df['NOM_ENT']):
        logging.info("NOM_ENT column is numeric, mapping to state names.")
        df['NOM_ENT'] = df['NOM_ENT'].map(estados)

    if not verificar_dataset(df):
        logging.error(f"The dataset for Y:{year} Q:{quarter} failed verification.")
        return

    df['NOM_ENT'] = df['NOM_ENT'].apply(normalizar_texto)
    df['NOM_MUN'] = df['NOM_MUN'].apply(normalizar_texto)

    df_yucatan = df[df['NOM_ENT'] == 'YUCATAN'].copy()

    if df_yucatan.empty:
        logging.info(f"No data found for YUCATAN in Y:{year} Q:{quarter}.")
        return

    df_yucatan['TOTAL_REGISTROS'] = 1
    df_yucatan['TOTAL_SEGUROS'] = (df_yucatan['BP1_1'] == 1).astype(int)
    df_yucatan['TOTAL_INSEGUROS'] = (df_yucatan['BP1_1'] == 2).astype(int)
    df_yucatan['TOTAL_NO_RESPONDE'] = (df_yucatan['BP1_1'] == 9).astype(int)
    
    df_yucatan = df_yucatan[df_yucatan['BP1_1'].isin([1, 2, 9])]

    if pd.api.types.is_numeric_dtype(df_yucatan['NOM_MUN']):
        logging.warning(f"NOM_MUN column is numeric for Y:{year} Q:{quarter}. A catalog is needed to map to municipality names.")

    resultado = df_yucatan.groupby('NOM_MUN').agg({
        'TOTAL_REGISTROS': 'sum',
        'TOTAL_SEGUROS': 'sum',
        'TOTAL_INSEGUROS': 'sum',
        'TOTAL_NO_RESPONDE': 'sum'
    }).reset_index()

    resultado['PCT_SEGUROS'] = (resultado['TOTAL_SEGUROS'] / resultado['TOTAL_REGISTROS']) * 100
    resultado['PCT_INSEGUROS'] = (resultado['TOTAL_INSEGUROS'] / resultado['TOTAL_REGISTROS']) * 100
    resultado['PCT_NO_RESPONDE'] = (resultado['TOTAL_NO_RESPONDE'] / resultado['TOTAL_REGISTROS']) * 100

    resultado['AÑO'] = year
    resultado['TRIMESTRE'] = quarter
    resultado['NOM_ENT'] = 'YUCATAN'
    
    columnas_finales = [
        'NOM_ENT', 'NOM_MUN', 'TOTAL_REGISTROS', 'TOTAL_SEGUROS', 
        'TOTAL_INSEGUROS', 'TOTAL_NO_RESPONDE', 'PCT_SEGUROS', 
        'PCT_INSEGUROS', 'PCT_NO_RESPONDE', 'AÑO', 'TRIMESTRE'
    ]
    resultado = resultado[columnas_finales]

    output_filename = f"procesado_{year}_{quarter}_cb.csv"
    output_path = os.path.join(output_dir, output_filename)
    resultado.to_csv(output_path, index=False, encoding='utf-8')
    logging.info(f"Processed file saved to: {output_path}")