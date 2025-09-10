import pandas as pd
import glob
import os
import argparse

def manage_empty_files(delete_files=False):
    """
    Identifica y opcionalmente elimina archivos CSV vacíos (sin filas de datos).
    """
    processed_files_pattern = "data/processed/processed_ensu_*.csv"
    file_list = glob.glob(processed_files_pattern)

    if not file_list:
        print("No se encontraron archivos para analizar.")
        return

    empty_files = []
    for file_path in file_list:
        try:
            df = pd.read_csv(file_path)
            if df.empty:
                empty_files.append(file_path)
        except pd.errors.EmptyDataError:
            # El archivo está completamente vacío (ni siquiera cabeceras)
            empty_files.append(file_path)
        except Exception as e:
            print(f"Error analizando el archivo {file_path}: {e}")

    if not empty_files:
        print("No se encontraron archivos vacíos.")
        return

    print(f"Se encontraron {len(empty_files)} archivos vacíos:")
    for f in empty_files:
        print(f"- {f}")

    if delete_files:
        print("\nEliminando archivos...")
        for f in empty_files:
            try:
                os.remove(f)
                print(f"  -> Eliminado: {f}")
            except Exception as e:
                print(f"  -> Error al eliminar {f}: {e}")
        print("\nLimpieza completada.")
    else:
        print("\nModo de solo lectura. No se eliminó ningún archivo.")
        print("Ejecute con el argumento --delete para eliminar estos archivos.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Identifica y/o elimina archivos CSV vacíos.")
    parser.add_argument("--delete", action="store_true", help="Activa el modo de eliminación de archivos.")
    args = parser.parse_args()

    manage_empty_files(delete_files=args.delete)
