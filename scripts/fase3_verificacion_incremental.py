import os
import json
import logging
import logging.handlers
from datetime import datetime

def setup_logging():
    log_dir = 'logs'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    log_filename = os.path.join(log_dir, "fase3_verificacion.log")
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.handlers.TimedRotatingFileHandler(
                log_filename,
                when='midnight',
                interval=1,
                backupCount=7
            ),
            logging.StreamHandler()
        ]
    )

def check_processed_files():
    logging.info("Starting Phase 3: Passing all validated files for processing.")
    
    try:
        with open('temp/archivos_validados.json', 'r', encoding='utf-8') as f:
            validated_data = json.load(f)
    except FileNotFoundError:
        logging.error("archivos_validados.json not found. Please run Phase 2 first.")
        return

    archivos_a_procesar = validated_data.get('archivos_seleccionados', [])
    
    output_data = {
        "timestamp": datetime.now().isoformat(),
        "total_archivos_validados": len(archivos_a_procesar),
        "archivos_ya_procesados": 0,
        "archivos_pendientes": len(archivos_a_procesar),
        "lista_pendientes": archivos_a_procesar
    }

    output_path = 'temp/archivos_pendientes.json'
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=4, ensure_ascii=False)

    logging.info(f"Phase 3: Complete. All validated files have been marked as pending. Pending files list saved to {output_path}")

if __name__ == "__main__":
    setup_logging()
    check_processed_files()