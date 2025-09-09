#!/usr/bin/env python3
"""
Fase 3: Verificación Incremental
Verifica qué archivos ya han sido procesados para evitar reprocesamiento
"""

import json
import os
from pathlib import Path
import logging
from datetime import datetime

# Configurar logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/fase3_verificacion.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def obtener_archivos_procesados():
    """
    Obtiene la lista de archivos ya procesados
    """
    directorio_procesados = 'data/yucatan_processed'
    archivos_procesados = []
    
    if Path(directorio_procesados).exists():
        for archivo in os.listdir(directorio_procesados):
            if archivo.endswith('.csv'):
                # Extraer información del nombre del archivo
                # Formato esperado: yucatan_2024_T1_CB0324.csv
                partes = archivo.replace('.csv', '').split('_')
                if len(partes) >= 4:
                    info = {
                        'archivo_procesado': archivo,
                        'año': partes[1],
                        'trimestre': partes[2],
                        'codigo_cb': partes[3] if len(partes) > 3 else None
                    }
                    archivos_procesados.append(info)
                    logger.debug(f"Archivo procesado encontrado: {archivo}")
    
    logger.info(f"Total de archivos ya procesados: {len(archivos_procesados)}")
    return archivos_procesados

def verificar_pendientes(archivos_validados, archivos_procesados):
    """
    Identifica qué archivos aún no han sido procesados
    """
    # Crear un set de trimestres ya procesados
    trimestres_procesados = set()
    for procesado in archivos_procesados:
        clave = f"{procesado['año']}_{procesado['trimestre']}"
        trimestres_procesados.add(clave)
        logger.debug(f"Trimestre ya procesado: {clave}")
    
    # Identificar archivos pendientes
    archivos_pendientes = []
    archivos_ya_procesados = []
    
    for archivo in archivos_validados:
        if archivo.get('trimestre_info'):
            info = archivo['trimestre_info']
            clave = f"{info['año']}_T{info['trimestre']}"
            
            if clave not in trimestres_procesados:
                archivo['estado'] = 'pendiente'
                archivo['razon'] = 'No procesado anteriormente'
                archivos_pendientes.append(archivo)
                logger.info(f"Archivo pendiente: {archivo['nombre']} ({clave})")
            else:
                archivo['estado'] = 'procesado'
                archivo['razon'] = f'Ya procesado como {clave}'
                archivos_ya_procesados.append(archivo)
                logger.debug(f"Archivo ya procesado: {archivo['nombre']} ({clave})")
        else:
            # Si no tiene información de trimestre, marcarlo como pendiente
            archivo['estado'] = 'pendiente'
            archivo['razon'] = 'Sin información de trimestre'
            archivos_pendientes.append(archivo)
            logger.warning(f"Archivo sin trimestre identificado: {archivo['nombre']}")
    
    return archivos_pendientes, archivos_ya_procesados

def main():
    """
    Función principal de la Fase 3
    """
    logger.info("=== FASE 3: VERIFICACIÓN INCREMENTAL ===")
    
    # Cargar archivos validados de Fase 2
    validados_path = 'temp/archivos_validados.json'
    if not Path(validados_path).exists():
        logger.error(f"No se encontró el archivo de validación en {validados_path}")
        logger.error("Ejecute primero la Fase 2")
        return False
    
    with open(validados_path, 'r', encoding='utf-8') as f:
        datos_validacion = json.load(f)
    
    archivos_validados = datos_validacion['archivos_validos']
    logger.info(f"Archivos validados a verificar: {len(archivos_validados)}")
    
    # Obtener archivos ya procesados
    archivos_procesados = obtener_archivos_procesados()
    
    # Verificar pendientes
    pendientes, ya_procesados = verificar_pendientes(archivos_validados, archivos_procesados)
    
    # Guardar resultados
    resultado_verificacion = {
        'fecha_verificacion': datetime.now().isoformat(),
        'total_validados': len(archivos_validados),
        'total_ya_procesados': len(ya_procesados),
        'total_pendientes': len(pendientes),
        'archivos_pendientes': pendientes,
        'archivos_ya_procesados': ya_procesados,
        'directorio_procesados': 'data/yucatan_processed'
    }
    
    # Guardar archivo de pendientes
    pendientes_path = 'temp/archivos_pendientes.json'
    with open(pendientes_path, 'w', encoding='utf-8') as f:
        json.dump(resultado_verificacion, f, indent=2, ensure_ascii=False)
    
    logger.info(f"Resultados guardados en: {pendientes_path}")
    
    # Resumen
    logger.info("=== RESUMEN FASE 3 ===")
    logger.info(f"Total archivos validados: {len(archivos_validados)}")
    logger.info(f"Archivos ya procesados: {len(ya_procesados)}")
    logger.info(f"Archivos pendientes de procesar: {len(pendientes)}")
    
    if pendientes:
        logger.info("Archivos pendientes:")
        for archivo in pendientes[:5]:  # Mostrar solo los primeros 5
            logger.info(f"  - {archivo['nombre']} ({archivo.get('razon', 'Sin razón')})")
        if len(pendientes) > 5:
            logger.info(f"  ... y {len(pendientes) - 5} más")
    else:
        logger.info("No hay archivos pendientes. Todo está procesado.")
    
    # Política: Procesar aunque no haya pendientes (para regeneración)
    if len(pendientes) == 0:
        logger.warning("NOTA: No hay archivos nuevos, pero se puede continuar para regenerar")
    
    return True  # Siempre retornar True para continuar el flujo

if __name__ == "__main__":
    exito = main()
    exit(0 if exito else 1)