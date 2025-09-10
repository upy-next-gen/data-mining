
import pandas as pd
import glob
import os

def standardize_yucatan_name():
    """
    Itera sobre cada archivo de datos procesados de ENSU y estandariza el identificador de Yucatán,
    reemplazando el código '31' por 'YUCATAN'.
    """
    processed_files_pattern = "data/processed/processed_ensu_*.csv"

    # Encontrar todos los archivos de datos procesados
    file_list = glob.glob(processed_files_pattern)

    if not file_list:
        print(f"Error: No se encontraron archivos con el patrón '{processed_files_pattern}'")
        return

    print(f"Se encontraron {len(file_list)} archivos para estandarizar.")

    for file_path in file_list:
        try:
            # Verificar si el archivo está vacío o solo tiene cabeceras para evitar errores
            if os.path.getsize(file_path) < 100: # Umbral pequeño para archivos casi vacíos
                print(f"Omitiendo archivo vacío o casi vacío: {file_path}")
                continue

            print(f"Procesando: {file_path}")
            df = pd.read_csv(file_path, low_memory=False)

            if 'NOM_ENT' not in df.columns:
                print(f"  -> Advertencia: La columna 'NOM_ENT' no existe. Omitiendo archivo.")
                continue
            
            # Asegurarse de que la columna es de tipo string
            df['NOM_ENT'] = df['NOM_ENT'].astype(str)

            # Contar cuántos '31' hay antes de reemplazar
            replacements_count = (df['NOM_ENT'] == '31').sum()

            if replacements_count > 0:
                # Reemplazar '31' por 'YUCATAN'
                df['NOM_ENT'] = df['NOM_ENT'].str.replace('31', 'YUCATAN')
                # Sobrescribir el archivo original
                df.to_csv(file_path, index=False)
                print(f"  -> Se realizaron {replacements_count} reemplazos. Archivo actualizado.")
            else:
                print("  -> No se encontraron códigos '31' para reemplazar.")

        except Exception as e:
            print(f"  -> Error procesando el archivo {file_path}: {e}")

    print("\nProceso de estandarización finalizado.")

if __name__ == "__main__":
    standardize_yucatan_name()
