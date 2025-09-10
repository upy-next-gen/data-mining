import pandas as pd
import glob
import os

def clean_and_filter_in_place():
    """
    Itera sobre cada archivo de datos procesados de ENSU, filtra los datos para Yucatán
    (manejando tanto nombres como códigos numéricos) y sobrescribe el archivo original.
    """
    processed_files_pattern = "data/processed/processed_ensu_*.csv"

    # Encontrar todos los archivos de datos procesados
    file_list = glob.glob(processed_files_pattern)

    if not file_list:
        print(f"Error: No se encontraron archivos con el patrón '{processed_files_pattern}'")
        return

    print(f"Se encontraron {len(file_list)} archivos para procesar y sobrescribir.")

    for file_path in file_list:
        try:
            print(f"Procesando: {file_path}")
            df = pd.read_csv(file_path, low_memory=False)

            if 'NOM_ENT' not in df.columns:
                print(f"  -> Advertencia: La columna 'NOM_ENT' no existe. Omitiendo archivo.")
                continue

            # Forzar la columna a string para manejar tipos de datos mixtos (texto y numérico)
            df['NOM_ENT'] = df['NOM_ENT'].astype(str)

            # Filtrar por Yucatán usando el nombre o el código numérico (31)
            df_yucatan = df[df['NOM_ENT'].str.strip().str.upper().isin(['YUCATAN', 'YUCATÁN', '31'])].copy()

            # Sobrescribir el archivo original
            df_yucatan.to_csv(file_path, index=False)
            print(f"  -> Archivo actualizado con {df_yucatan.shape[0]} registros de Yucatán.")

        except Exception as e:
            print(f"  -> Error procesando el archivo {file_path}: {e}")

    print("\nProceso de limpieza finalizado.")

if __name__ == "__main__":
    clean_and_filter_in_place()
