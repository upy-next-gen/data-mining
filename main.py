#!/usr/bin/env python3
"""
Script de procesamiento de datos de percepción de seguridad ENSU
Genera estadísticas agregadas por entidad y municipio
"""

import os
import re
import csv
import logging
import unicodedata
from pathlib import Path
from datetime import datetime
import json

def configurar_logging():
    """Configura el sistema de logging verboso"""
    # Crear carpeta de logs si no existe
    log_dir = Path("data/yucatan-inseguridad/logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    
    # Configurar formato de log
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = log_dir / f"procesamiento_log_{timestamp}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    
    return logging.getLogger(__name__)

def normalizar_string(texto):
    """Normaliza strings: mayúsculas, sin acentos, Ñ→N"""
    if not texto or texto == '' or texto == 'nan':
        return ''
    
    # Convertir a string si no lo es
    texto = str(texto).strip()
    
    # Convertir a mayúsculas
    texto = texto.upper()
    
    # Eliminar acentos
    texto = unicodedata.normalize('NFD', texto)
    texto = ''.join(char for char in texto if unicodedata.category(char) != 'Mn')
    
    # Convertir Ñ a N
    texto = texto.replace('Ñ', 'N')
    
    # Limpiar espacios múltiples
    texto = ' '.join(texto.split())
    
    return texto

def extraer_año_trimestre(archivo_path):
    """Extrae año y trimestre del nombre del archivo"""
    nombre = os.path.basename(archivo_path)
    logger = logging.getLogger(__name__)
    
    # Patrones comunes en los nombres de archivo
    patrones = [
        # Patrón cb_MMAA o cb_MMAAAA
        (r'cb_(\d{2})(\d{2,4})', lambda m: (f"20{m.group(2)}" if len(m.group(2)) == 2 else m.group(2), obtener_trimestre(m.group(1)))),
        # Patrón cb_MMYY
        (r'cb_(\d{2})(\d{2})(?!\d)', lambda m: (f"20{m.group(2)}", obtener_trimestre(m.group(1)))),
        # Patrón YYYY_Qt
        (r'(\d{4})_(\d)t', lambda m: (m.group(1), f"Q{m.group(2)}")),
        # Patrón _MM_YYYY
        (r'_(\d{2})_(\d{4})', lambda m: (m.group(2), obtener_trimestre(m.group(1)))),
        # Patrón ensu_YYYY
        (r'ensu_(\d{4})', lambda m: (m.group(1), "DESCONOCIDO")),
    ]
    
    for patron, extractor in patrones:
        match = re.search(patron, nombre, re.IGNORECASE)
        if match:
            año, trimestre = extractor(match)
            logger.info(f"  Identificado: Año={año}, Trimestre={trimestre} (patrón: {patron})")
            return año, trimestre
    
    # Si no se encuentra patrón, intentar extraer año de la ruta completa
    match_año = re.search(r'20\d{2}', archivo_path)
    if match_año:
        año = match_año.group()
        logger.warning(f"  Solo se pudo identificar el año: {año}, trimestre desconocido")
        return año, "DESCONOCIDO"
    
    logger.warning(f"  No se pudo identificar año y trimestre para: {nombre}")
    return "DESCONOCIDO", "DESCONOCIDO"

def obtener_trimestre(mes_str):
    """Convierte mes a trimestre"""
    try:
        mes = int(mes_str)
        if 1 <= mes <= 3:
            return "Q1"
        elif 4 <= mes <= 6:
            return "Q2"
        elif 7 <= mes <= 9:
            return "Q3"
        elif 10 <= mes <= 12:
            return "Q4"
    except:
        pass
    return "DESCONOCIDO"

def procesar_archivo(archivo_path, output_dir):
    """Procesa un archivo CSV y genera estadísticas agregadas"""
    logger = logging.getLogger(__name__)
    logger.info(f"\nProcesando archivo: {archivo_path}")
    
    # Extraer año y trimestre
    año, trimestre = extraer_año_trimestre(archivo_path)
    
    # Estadísticas del archivo
    stats = {
        'total_registros_originales': 0,
        'registros_validos': 0,
        'registros_bp1_1_null': 0,
        'registros_bp1_1_fuera_rango': 0,
        'valores_fuera_rango': {}
    }
    
    # Diccionario para almacenar datos agregados por entidad-municipio
    agregados = {}
    
    try:
        with open(archivo_path, 'r', encoding='utf-8', errors='ignore') as f:
            reader = csv.DictReader(f)
            
            for row_num, row in enumerate(reader, 1):
                stats['total_registros_originales'] += 1
                
                # Obtener valores de las columnas
                nom_ent = row.get('NOM_ENT', '').strip()
                nom_mun = row.get('NOM_MUN', '').strip()
                bp1_1 = row.get('BP1_1', '').strip()
                
                # Validar BP1_1
                if not bp1_1 or bp1_1 == '':
                    stats['registros_bp1_1_null'] += 1
                    if row_num <= 10:  # Log solo primeros casos
                        logger.debug(f"    Fila {row_num}: BP1_1 vacío/null")
                    continue
                
                try:
                    bp1_1_valor = int(bp1_1)
                except ValueError:
                    stats['registros_bp1_1_fuera_rango'] += 1
                    valor_str = str(bp1_1)
                    stats['valores_fuera_rango'][valor_str] = stats['valores_fuera_rango'].get(valor_str, 0) + 1
                    if row_num <= 10:
                        logger.debug(f"    Fila {row_num}: BP1_1 no numérico: '{bp1_1}'")
                    continue
                
                if bp1_1_valor not in [1, 2, 9]:
                    stats['registros_bp1_1_fuera_rango'] += 1
                    stats['valores_fuera_rango'][str(bp1_1_valor)] = stats['valores_fuera_rango'].get(str(bp1_1_valor), 0) + 1
                    if row_num <= 10:
                        logger.debug(f"    Fila {row_num}: BP1_1={bp1_1_valor} fuera de rango [1,2,9]")
                    continue
                
                # Normalizar strings
                nom_ent_norm = normalizar_string(nom_ent)
                nom_mun_norm = normalizar_string(nom_mun)
                
                # Crear clave de agrupación
                clave = (nom_ent_norm, nom_mun_norm)
                
                # Inicializar contador si no existe
                if clave not in agregados:
                    agregados[clave] = {
                        'total': 0,
                        'seguros': 0,
                        'inseguros': 0,
                        'no_responde': 0
                    }
                
                # Actualizar contadores
                agregados[clave]['total'] += 1
                stats['registros_validos'] += 1
                
                if bp1_1_valor == 1:
                    agregados[clave]['seguros'] += 1
                elif bp1_1_valor == 2:
                    agregados[clave]['inseguros'] += 1
                elif bp1_1_valor == 9:
                    agregados[clave]['no_responde'] += 1
        
        # Log de estadísticas
        logger.info(f"  Total registros originales: {stats['total_registros_originales']:,}")
        logger.info(f"  Registros válidos procesados: {stats['registros_validos']:,}")
        logger.info(f"  Registros excluidos:")
        logger.info(f"    - BP1_1 null/vacío: {stats['registros_bp1_1_null']:,}")
        logger.info(f"    - BP1_1 fuera de rango: {stats['registros_bp1_1_fuera_rango']:,}")
        
        if stats['valores_fuera_rango']:
            logger.warning(f"  Valores fuera de rango encontrados: {stats['valores_fuera_rango']}")
        
        # Generar archivo de salida
        if agregados:
            output_file = output_dir / f"procesado_{año}_{trimestre}_cb.csv"
            
            with open(output_file, 'w', newline='', encoding='utf-8') as f:
                fieldnames = [
                    'NOM_ENT', 'NOM_MUN', 'TOTAL_REGISTROS',
                    'TOTAL_SEGUROS', 'TOTAL_INSEGUROS', 'TOTAL_NO_RESPONDE',
                    'PCT_SEGUROS', 'PCT_INSEGUROS', 'PCT_NO_RESPONDE',
                    'AÑO', 'TRIMESTRE'
                ]
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                
                # Ordenar por entidad y municipio
                for (nom_ent, nom_mun) in sorted(agregados.keys()):
                    datos = agregados[(nom_ent, nom_mun)]
                    total = datos['total']
                    
                    # Calcular porcentajes
                    pct_seguros = (datos['seguros'] / total * 100) if total > 0 else 0
                    pct_inseguros = (datos['inseguros'] / total * 100) if total > 0 else 0
                    pct_no_responde = (datos['no_responde'] / total * 100) if total > 0 else 0
                    
                    writer.writerow({
                        'NOM_ENT': nom_ent,
                        'NOM_MUN': nom_mun,
                        'TOTAL_REGISTROS': total,
                        'TOTAL_SEGUROS': datos['seguros'],
                        'TOTAL_INSEGUROS': datos['inseguros'],
                        'TOTAL_NO_RESPONDE': datos['no_responde'],
                        'PCT_SEGUROS': f"{pct_seguros:.2f}",
                        'PCT_INSEGUROS': f"{pct_inseguros:.2f}",
                        'PCT_NO_RESPONDE': f"{pct_no_responde:.2f}",
                        'AÑO': año,
                        'TRIMESTRE': trimestre
                    })
            
            logger.info(f"  >> Archivo generado: {output_file}")
            logger.info(f"  Total de grupos (entidad-municipio): {len(agregados)}")
            
            return True, stats
        else:
            logger.info(f"  >> No se encontraron registros válidos para procesar")
            return False, stats
            
    except Exception as e:
        logger.error(f"  >> Error procesando archivo: {str(e)}")
        return False, stats

def main():
    # Configurar logging
    logger = configurar_logging()
    
    logger.info("="*80)
    logger.info("PROCESAMIENTO DE PERCEPCIÓN DE SEGURIDAD - ENSU")
    logger.info("="*80)
    logger.info(f"Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Crear carpeta de salida
    output_dir = Path("data/yucatan-inseguridad")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Leer archivo de exploración para obtener archivos válidos
    archivos_validos = []
    
    # Usar el archivo de exploración proporcionado
    exploration_file = Path("exploration.json")
    if exploration_file.exists():
        logger.info(f"Usando archivo de exploración: {exploration_file}")
        
        with open(exploration_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            # Assuming the structure is a dict with a key 'archivos_validos'
            if isinstance(data, dict) and 'archivos_validos' in data:
                archivos_validos = data['archivos_validos']
            else:
                logger.error("El archivo JSON no tiene el formato esperado (diccionario con clave 'archivos_validos').")
                archivos_validos = []

    if not archivos_validos:
        logger.error("No se encontraron archivos válidos para procesar en el JSON.")
        return
    
    logger.info(f"Total de archivos a procesar: {len(archivos_validos)}")
    
    # Procesar cada archivo
    exitosos = 0
    fallidos = 0
    stats_globales = {
        'total_registros_procesados': 0,
        'total_registros_excluidos': 0
    }
    
    for i, archivo in enumerate(archivos_validos, 1):
        logger.info(f"\n[{i}/{len(archivos_validos)}] " + "="*60)
        exito, stats = procesar_archivo(archivo, output_dir)
        
        if exito:
            exitosos += 1
            stats_globales['total_registros_procesados'] += stats['registros_validos']
            stats_globales['total_registros_excluidos'] += (
                stats['registros_bp1_1_null'] + stats['registros_bp1_1_fuera_rango']
            )
        else:
            fallidos += 1
    
    # Resumen final
    logger.info("\n" + "="*80)
    logger.info("RESUMEN DE PROCESAMIENTO")
    logger.info("="*80)
    logger.info(f"Archivos procesados exitosamente: {exitosos}")
    logger.info(f"Archivos con errores: {fallidos}")
    logger.info(f"Total de registros válidos procesados: {stats_globales['total_registros_procesados']:,}")
    logger.info(f"Total de registros excluidos: {stats_globales['total_registros_excluidos']:,}")
    logger.info(f"Fin: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("="*80)
    
    print(f"\n>> Procesamiento completado. Ver logs en: data/yucatan-inseguridad/logs/")
    print(f">> Archivos procesados guardados en: data/yucatan-inseguridad/")

if __name__ == "__main__":
    main()
