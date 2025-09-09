#!/usr/bin/env python3
"""
Script de exploración para identificar datasets CB y sus columnas
"""

import os
import csv
from pathlib import Path
import json
from datetime import datetime

def buscar_archivos_cb(directorio_base="data"):
    """Busca todos los archivos CSV que contengan 'cb' en su nombre"""
    archivos_cb = []
    
    for root, dirs, files in os.walk(directorio_base):
        for file in files:
            if 'cb' in file.lower() and file.endswith('.csv'):
                # Filtrar solo archivos de conjunto_de_datos, no diccionarios
                if 'conjunto_de_datos' in root and 'diccionario' not in root:
                    archivos_cb.append(os.path.join(root, file))
    
    return sorted(archivos_cb)

def explorar_columnas(archivo_path, num_filas=5):
    """Lee las primeras filas de un archivo CSV y extrae información de columnas"""
    info = {
        'archivo': archivo_path,
        'nombre': os.path.basename(archivo_path),
        'columnas': [],
        'tiene_nom_ent': False,
        'tiene_nom_mun': False,
        'tiene_bp1_1': False,
        'primeras_filas': [],
        'error': None
    }
    
    try:
        with open(archivo_path, 'r', encoding='utf-8', errors='ignore') as f:
            reader = csv.DictReader(f)
            info['columnas'] = reader.fieldnames if reader.fieldnames else []
            
            # Verificar columnas requeridas (case-insensitive)
            columnas_upper = [col.upper() for col in info['columnas']]
            info['tiene_nom_ent'] = 'NOM_ENT' in columnas_upper
            info['tiene_nom_mun'] = 'NOM_MUN' in columnas_upper
            info['tiene_bp1_1'] = 'BP1_1' in columnas_upper
            
            # Leer primeras filas para muestra
            for i, row in enumerate(reader):
                if i >= num_filas:
                    break
                info['primeras_filas'].append(row)
                
    except Exception as e:
        info['error'] = str(e)
    
    return info

def main():
    print("=" * 80)
    print("EXPLORACIÓN DE DATASETS CB - PERCEPCIÓN DE SEGURIDAD")
    print("=" * 80)
    print(f"Fecha de ejecución: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # Buscar archivos
    print("Buscando archivos CB en carpeta 'data'...")
    archivos_cb = buscar_archivos_cb()
    print(f"Se encontraron {len(archivos_cb)} archivos CB\n")
    
    # Explorar cada archivo
    resultados = []
    archivos_validos = []
    archivos_invalidos = []
    
    for i, archivo in enumerate(archivos_cb, 1):
        print(f"[{i}/{len(archivos_cb)}] Explorando: {archivo}")
        info = explorar_columnas(archivo)
        resultados.append(info)
        
        # Clasificar archivo
        if info['tiene_nom_ent'] and info['tiene_nom_mun'] and info['tiene_bp1_1']:
            archivos_validos.append(archivo)
            print(f"  ✓ VÁLIDO - Contiene todas las columnas requeridas")
        else:
            archivos_invalidos.append(archivo)
            print(f"  ✗ INVÁLIDO - Faltan columnas:")
            if not info['tiene_nom_ent']:
                print(f"    - Falta NOM_ENT")
            if not info['tiene_nom_mun']:
                print(f"    - Falta NOM_MUN")
            if not info['tiene_bp1_1']:
                print(f"    - Falta BP1_1")
    
    # Resumen
    print("\n" + "=" * 80)
    print("RESUMEN DE EXPLORACIÓN")
    print("=" * 80)
    print(f"Total de archivos CB encontrados: {len(archivos_cb)}")
    print(f"Archivos VÁLIDOS (con todas las columnas): {len(archivos_validos)}")
    print(f"Archivos INVÁLIDOS (faltan columnas): {len(archivos_invalidos)}")
    
    # Guardar resultados en JSON
    output_file = f"exploracion_cb_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump({
            'fecha_exploracion': datetime.now().isoformat(),
            'total_archivos': len(archivos_cb),
            'archivos_validos': archivos_validos,
            'archivos_invalidos': archivos_invalidos,
            'detalles': resultados
        }, f, indent=2, ensure_ascii=False)
    
    print(f"\n✓ Resultados detallados guardados en: {output_file}")
    
    # Mostrar lista de archivos válidos
    if archivos_validos:
        print("\n" + "=" * 80)
        print("ARCHIVOS VÁLIDOS PARA PROCESAR:")
        print("=" * 80)
        for archivo in archivos_validos:
            print(f"  • {archivo}")
    
    return archivos_validos

if __name__ == "__main__":
    archivos_validos = main()
    print(f"\n✓ Exploración completada. {len(archivos_validos)} archivos listos para procesar.")