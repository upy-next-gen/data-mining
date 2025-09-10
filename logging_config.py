
import logging
import sys

def setup_logging():
    """
    Configura un logger dual para escribir a un archivo y a la consola.
    """
    # Crear un logger
    logger = logging.getLogger('data_processor')
    logger.setLevel(logging.DEBUG)  # Nivel más bajo para capturar todo

    # Evitar que se agreguen múltiples handlers si la función se llama más de una vez
    if logger.hasHandlers():
        return logger

    # Formateador para los logs
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    # Handler para escribir en el archivo de log
    file_handler = logging.FileHandler('processing.log', mode='w') # 'w' para sobrescribir el log en cada ejecución
    file_handler.setLevel(logging.DEBUG) # Registrar todo en el archivo
    file_handler.setFormatter(formatter)

    # Handler para mostrar en la consola
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(logging.INFO) # Mostrar solo INFO y niveles superiores en consola
    stream_handler.setFormatter(formatter)

    # Agregar handlers al logger
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    return logger

# Instancia global del logger
logger = setup_logging()
