import pandas as pd
import logging

def verificar_dataset(df):
    """
    Verifica que un DataFrame contenga las columnas necesarias y que los valores
    de 'BP1_1' estén en el rango esperado.

    Args:
        df (pd.DataFrame): El DataFrame a verificar.

    Returns:
        bool: True si la verificación es exitosa, False en caso contrario.
    """
    required_columns = ['NOM_ENT', 'NOM_MUN', 'BP1_1']
    missing_columns = [col for col in required_columns if col not in df.columns]

    if missing_columns:
        logging.warning(f"Faltan las siguientes columnas en el dataset: {missing_columns}")
        return False

    # Verificar valores en BP1_1
    valores_permitidos = {1, 2, 9}
    valores_actuales = set(df['BP1_1'].dropna().unique())

    if not valores_actuales.issubset(valores_permitidos):
        logging.warning(f"Valores inesperados en la columna 'BP1_1': {valores_actuales - valores_permitidos}")
    
    logging.info("La verificación de columnas y valores de BP1_1 ha sido exitosa.")
    return True