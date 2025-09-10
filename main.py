
import os
import glob
from process_data import process_survey_data
from logging_config import logger

def main():
    """
    Orquesta el proceso de descubrimiento y procesamiento de los datasets de encuestas.
    """
    try:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        # Nueva ruta de salida según lo solicitado
        output_dir = os.path.join(base_dir, 'data', 'yucatan-inseguridad')

        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            logger.info(f"Directorio de salida creado en: {output_dir}")

        # Patrón de búsqueda más específico para los archivos de datos principales
        search_pattern = os.path.join(base_dir, 'data', '**', '*cb*.csv')
        survey_files = glob.glob(search_pattern, recursive=True)

        # Filtrar explícitamente los diccionarios de datos
        survey_files = [f for f in survey_files if 'diccionario_de_datos' not in f.lower()]

        if not survey_files:
            logger.warning("No se encontraron archivos de encuestas que cumplan con el criterio en el directorio 'data'.")
            return

        logger.info(f"Se encontraron {len(survey_files)} archivos de datos para procesar.")

        for file_path in survey_files:
            process_survey_data(file_path, output_dir)

        logger.info("Procesamiento completado.")

    except Exception as e:
        logger.critical(f"Ha ocurrido un error inesperado en la ejecución principal: {e}", exc_info=True)

if __name__ == "__main__":
    main()
