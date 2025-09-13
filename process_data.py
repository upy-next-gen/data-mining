import pandas as pd
import os

def process_insecurity_data():
    """
    Processes the ENSU 2025 survey data to extract and filter
    information about the perception of insecurity.
    """
    # Define the path to the CSV file
    file_path = "data/conjunto_de_datos_ensu_2025_2t_csv/conjunto_de_datos_ensu_cb_0625/conjunto_de_datos/conjunto_de_datos_ensu_cb_0625.csv"

    # Check if the file exists
    if not os.path.exists(file_path):
        print(f"Error: El archivo no se encontró en la ruta {file_path}")
        return None

    # Read the CSV file using pandas
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        print(f"Error al leer el archivo CSV: {e}")
        return None

    # Select the required columns
    columns_to_keep = ['NOM_ENT', 'NOM_MUN', 'NOM_CD', 'BP1_1']
    df_selected = df[columns_to_keep]

    # Filter the DataFrame based on BP1_1 values
    valid_bp1_1_values = [1, 2, 9]
    df_filtered = df_selected[df_selected['BP1_1'].isin(valid_bp1_1_values)].copy()

    # Filter by state ('YUCATAN')
    df_filtered = df_filtered[df_filtered['NOM_ENT'] == 'YUCATAN']

    print("Procesamiento inicial completado. Columnas seleccionadas y datos filtrados.")
    print("Primeras 5 filas del nuevo dataset:")
    print(df_filtered.head())

    return df_filtered

if __name__ == "__main__":
    processed_df = process_insecurity_data()
    if processed_df is not None:
        print("\nDataset procesado exitosamente.")
