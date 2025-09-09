# Flujo de Procesamiento ENSU - Análisis de Percepción de Seguridad en Yucatán

## Tabla de Contenidos
1. [Introducción](#introducción)
2. [Pre-requisitos](#pre-requisitos)
3. [Estructura de Directorios](#estructura-de-directorios)
4. [Fase 1: Mapeo y Descubrimiento](#fase-1-mapeo-y-descubrimiento)
5. [Fase 2: Validación](#fase-2-validación)
6. [Fase 3: Verificación Incremental](#fase-3-verificación-incremental)
7. [Fase 4: Procesamiento](#fase-4-procesamiento)
8. [Fase 5: Reporte Final](#fase-5-reporte-final)
9. [Ejecución Completa](#ejecución-completa)
10. [Troubleshooting](#troubleshooting)
11. [Anexos](#anexos)

---

## Introducción

Este documento describe el proceso completo para analizar datos de la Encuesta Nacional de Seguridad Pública Urbana (ENSU) específicamente para el estado de Yucatán. El proceso está dividido en 5 fases independientes pero conectadas, cada una con su propio script Python.

### Objetivo General
Procesar múltiples trimestres de datos ENSU para:
- Extraer datos de percepción de seguridad (columna BP1_1)
- Filtrar información específica de Yucatán
- Calcular estadísticas agregadas por municipio y ciudad
- Generar reportes de tendencias temporales

### Flujo de Datos
```
data/ (archivos ENSU raw) 
    ↓ [Fase 1]
temp/mapeo_archivos.json 
    ↓ [Fase 2]
temp/archivos_validados.json 
    ↓ [Fase 3]
temp/archivos_pendientes.json 
    ↓ [Fase 4]
data/yucatan_processed/*.csv 
    ↓ [Fase 5]
reports/reporte_final_YYYYMMDD.html + reports/resumen_ejecutivo.json
```

### Columnas Requeridas vs Opcionales

**Columnas REQUERIDAS (obligatorias):**
- `NOM_ENT`: Nombre de la entidad federativa (⚠️ IMPORTANTE: formato varía por año - ver nota abajo)
- `NOM_MUN`: Nombre del municipio  
- `BP1_1`: Percepción de seguridad (1=Seguro, 2=Inseguro, 9=No responde)

**Columnas OPCIONALES:**
- `NOM_CD`: Nombre de la ciudad (si no existe, se usa "SIN_CIUDAD" como valor por defecto)

### ⚠️ NOTA CRÍTICA: Formato de NOM_ENT

El valor de "Yucatán" en NOM_ENT ha cambiado a través de los años:
- **2016-2017**: `'Yucatán\r'` (con tilde y retorno de carro \r)
- **2018-2021**: `'Yucatan\r'` o `'Yucatan'` (sin tilde, con o sin \r)
- **2022-2025**: `'YUCATAN'` (mayúsculas sin tilde)

El pipeline maneja automáticamente estas variaciones usando normalización:
```python
df['NOM_ENT'].str.strip().str.upper().str.contains('YUCAT')
```

---

## Pre-requisitos

### Instalación de uv (si no está instalado)
```bash
# Instalar uv (gestor de paquetes y entornos Python)
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### Configuración del Proyecto con uv
```bash
# Inicializar proyecto con uv (si no existe pyproject.toml)
uv init security-perception

# Crear entorno virtual con Python 3.13
uv venv --python 3.13

# IMPORTANTE: El nombre del proyecto debe ser 'security-perception'
# Esto se verifica en el archivo pyproject.toml

# El entorno se activa automáticamente al usar 'uv run'
# No es necesario activar manualmente el entorno

# Agregar dependencias al proyecto
uv add pandas

# Nota: pathlib, json, os, re, logging son módulos estándar de Python 3.x, no requieren instalación
# numpy NO es necesario para este proyecto
```

### Verificación del Entorno
```bash
# Verificar que uv está instalado
which uv

# Verificar versión de Python en el entorno
uv run python --version

# Verificar dependencias instaladas
uv pip list
```

### Estructura de Carpetas Requerida
```
proyecto/
├── data/                         # Carpeta con archivos ENSU originales
│   └── conjunto_de_datos_*/      # Subcarpetas con datos por trimestre
├── data/yucatan_processed/       # Salida de archivos procesados
├── logs/                         # Logs de ejecución
├── temp/                         # Archivos intermedios JSON
├── reports/                      # Reportes HTML y JSON finales
└── scripts/                      # Scripts de las 5 fases
```

### Crear estructura inicial:
```bash
# IMPORTANTE: Crear todos los directorios necesarios antes de ejecutar los scripts
mkdir -p logs temp data/yucatan_processed reports scripts
```

### Nota Importante sobre Ejecución
**TODOS los comandos Python en este documento deben ejecutarse con `uv run` para usar el entorno virtual correcto:**
```bash
# En lugar de: python script.py
# Usar: uv run python script.py
```

---

## Fase 1: Mapeo y Descubrimiento

### Objetivo
Identificar y catalogar todos los archivos ENSU disponibles en la carpeta `data/`, extrayendo metadata relevante.

### Script: `fase1_mapeo_descubrimiento.py`

#### Entradas
- Directorio: `data/`
- Patrón de búsqueda: archivos que contengan los patrones `_cb_`, `conjunto_de_datos_cb` o `ensu_cb` (mediante regex)

#### Salidas
- `temp/mapeo_archivos.json`: Catálogo de archivos encontrados
- `logs/fase1_mapeo_YYYYMMDD_HHMMSS.log`: Log de ejecución

#### Código del Script

**IMPORTANTE**: Guarda el siguiente código en el archivo `scripts/fase1_mapeo_descubrimiento.py`

```python
#!/usr/bin/env python3
"""
Fase 1: Mapeo y Descubrimiento de archivos ENSU
"""

import os
import json
import logging
from datetime import datetime
from pathlib import Path
import pandas as pd
import re

def setup_logging():
    """Configurar sistema de logging"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = f"logs/fase1_mapeo_{timestamp}.log"
    
    os.makedirs("logs", exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] %(levelname)s: %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def extract_period_from_filename(filepath):
    """Extraer año y trimestre del nombre del archivo"""
    filename = str(filepath)
    
    # Patrones comunes en nombres de archivo ENSU
    patterns = [
        r'(\d{4})_(\d)t',  # 2025_2t
        r'ensu_(\d{2})(\d{2})',  # ensu_0625 (MMYY)
        r'(\d{4}).*[Tt](\d)',  # 2024T3
    ]
    
    for pattern in patterns:
        match = re.search(pattern, filename)
        if match:
            if len(match.group(1)) == 2:  # Formato MMYY
                month = int(match.group(1))
                year = 2000 + int(match.group(2))
                trimestre = (month - 1) // 3 + 1
            else:
                year = int(match.group(1))
                trimestre = int(match.group(2))
            return year, trimestre
    
    return None, None

def extract_period_from_csv(filepath):
    """Extraer período del contenido del CSV"""
    try:
        # Leer 5 filas para buscar columna de período (estandarizado)
        df = pd.read_csv(filepath, nrows=5)
        
        # Buscar columna PER o similar
        period_columns = ['PER', 'PERIODO', 'periodo', 'per']
        for col in period_columns:
            if col in df.columns and not df[col].isna().all():
                period_value = str(df[col].iloc[0])
                # Formato esperado: MMYY
                if len(period_value) == 4 and period_value.isdigit():
                    month = int(period_value[:2])
                    year = 2000 + int(period_value[2:])
                    trimestre = (month - 1) // 3 + 1
                    return year, trimestre
        
        return None, None
    except Exception as e:
        logging.error(f"Error leyendo {filepath}: {e}")
        return None, None

def scan_directory(base_path):
    """Escanear directorio en busca de archivos ENSU CB"""
    logger = logging.getLogger(__name__)
    logger.info(f"=== INICIANDO ESCANEO DE DIRECTORIO: {base_path} ===")
    logger.info(f"Directorio absoluto: {os.path.abspath(base_path)}")
    
    archivos_encontrados = []
    total_csvs_encontrados = 0
    
    # Patrón más específico para archivos CB
    cb_pattern = re.compile(r'(_cb_|conjunto_de_datos_cb|ensu_cb)', re.IGNORECASE)
    
    for root, dirs, files in os.walk(base_path):
        csv_files = [f for f in files if f.endswith('.csv')]
        if csv_files:
            logger.info(f"Directorio {root}: {len(csv_files)} archivos CSV encontrados")
            total_csvs_encontrados += len(csv_files)
        
        for file in files:
            if file.endswith('.csv') and cb_pattern.search(file):
                filepath = Path(root) / file
                logger.info(f"Archivo CB identificado: {filepath}")
                logger.info(f"  - Tamaño: {os.path.getsize(filepath) / (1024*1024):.2f} MB")
                
                # Extraer metadata
                try:
                    # Intentar extraer período del nombre
                    year_from_name, trimestre_from_name = extract_period_from_filename(filepath)
                    identificacion_metodo = 'filename' if year_from_name else None
                    
                    # Si no se encuentra en el nombre, buscar en el contenido
                    year = year_from_name
                    trimestre = trimestre_from_name
                    
                    if year is None:
                        logger.info(f"  - Período no encontrado en nombre, analizando contenido CSV...")
                        year, trimestre = extract_period_from_csv(filepath)
                        if year:
                            identificacion_metodo = 'contenido'
                            logger.info(f"  - Período extraído del contenido: {year} Q{trimestre}")
                        else:
                            identificacion_metodo = 'no_identificado'
                            logger.warning(f"  - ⚠ No se pudo identificar el período")
                    else:
                        logger.info(f"  - Período extraído del nombre: {year} Q{trimestre}")
                    
                    # Obtener información del archivo
                    file_stats = os.stat(filepath)
                    
                    metadata = {
                        'filepath': str(filepath),
                        'filename': file,
                        'size_mb': round(file_stats.st_size / (1024 * 1024), 2),
                        'modified_date': datetime.fromtimestamp(file_stats.st_mtime).isoformat(),
                        'year': year,
                        'trimestre': trimestre,
                        'periodo_str': f"{year}_Q{trimestre}" if year else f"UNKNOWN_{len(archivos_encontrados)+1:03d}",  # NOTA: Períodos no identificados se marcan como UNKNOWN_XXX,
                        'identificacion_metodo': identificacion_metodo
                    }
                    
                    # Verificar columnas básicas
                    try:
                        # Leer 5 filas para verificación básica (estandarizado)
                        df_sample = pd.read_csv(filepath, nrows=5)
                        metadata['columnas_muestra'] = list(df_sample.columns)[:10]
                        metadata['total_columnas'] = len(df_sample.columns)
                        
                        # Verificar columnas requeridas
                        required_cols = ['NOM_ENT', 'NOM_MUN', 'NOM_CD', 'BP1_1']
                        cols_presentes = [col for col in required_cols if col in df_sample.columns]
                        cols_faltantes = [col for col in required_cols if col not in df_sample.columns]
                        
                        metadata['columnas_requeridas_presentes'] = cols_presentes
                        metadata['columnas_requeridas_faltantes'] = cols_faltantes
                        
                        if cols_faltantes:
                            logger.warning(f"  - ⚠ Columnas requeridas faltantes: {cols_faltantes}")
                        else:
                            logger.info(f"  - ✓ Todas las columnas requeridas presentes")
                            
                    except Exception as e:
                        logger.error(f"  - Error al leer columnas: {e}")
                        metadata['columnas_muestra'] = []
                        metadata['total_columnas'] = 0
                        metadata['columnas_requeridas_presentes'] = []
                        metadata['columnas_requeridas_faltantes'] = []
                    
                    archivos_encontrados.append(metadata)
                    logger.info(f"  - Metadata completa para período: {metadata['periodo_str']}")
                    
                except Exception as e:
                    logger.error(f"  - ERROR CRÍTICO procesando {filepath}: {e}")
                    logger.error(f"    Tipo de error: {type(e).__name__}")
                    import traceback
                    logger.error(f"    Stack trace: {traceback.format_exc()}")
    
    logger.info(f"=== RESUMEN DE ESCANEO ===")
    logger.info(f"Total de archivos CSV en el directorio: {total_csvs_encontrados}")
    logger.info(f"Total de archivos CB encontrados: {len(archivos_encontrados)}")
    logger.info(f"Archivos con período identificado: {sum(1 for a in archivos_encontrados if a['identificacion_metodo'] != 'no_identificado')}")
    
    return archivos_encontrados

def save_mapping(archivos, output_path="temp/mapeo_archivos.json"):
    """Guardar mapeo en archivo JSON"""
    logger = logging.getLogger(__name__)
    
    # Asegurar que el directorio temp existe
    os.makedirs("temp", exist_ok=True)
    
    output_data = {
        'timestamp': datetime.now().isoformat(),
        'total_archivos': len(archivos),
        'archivos': archivos
    }
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    
    logger.info(f"Mapeo guardado en: {output_path}")
    logger.info(f"Total de archivos encontrados: {len(archivos)}")
    
    # Resumen por período
    periodos = {}
    for archivo in archivos:
        periodo = archivo['periodo_str']
        if periodo not in periodos:
            periodos[periodo] = []
        periodos[periodo].append(archivo['filename'])
    
    logger.info("=== RESUMEN POR PERÍODO ===")
    for periodo, files in sorted(periodos.items()):
        logger.info(f"  {periodo}: {len(files)} archivo(s)")
        if len(files) > 1:
            logger.warning(f"    ADVERTENCIA: Múltiples archivos para {periodo}")

def main():
    logger = setup_logging()
    
    try:
        # Escanear directorio
        archivos = scan_directory("data")
        
        # Guardar mapeo
        save_mapping(archivos)
        
        logger.info("=== FASE 1 COMPLETADA EXITOSAMENTE ===")
        
    except Exception as e:
        logger.error(f"Error fatal en Fase 1: {e}")
        raise

if __name__ == "__main__":
    main()
```

#### Formato de Salida: `mapeo_archivos.json`
```json
{
  "timestamp": "2024-09-04T14:30:00",
  "total_archivos": 3,
  "archivos": [
    {
      "filepath": "data/conjunto_de_datos_ensu_2025_2t_csv/conjunto_de_datos_ensu_cb_0625/conjunto_de_datos/conjunto_de_datos_ensu_cb_0625.csv",
      "filename": "conjunto_de_datos_ensu_cb_0625.csv",
      "size_mb": 15.85,
      "modified_date": "2024-07-22T13:25:00",
      "year": 2025,
      "trimestre": 2,
      "periodo_str": "2025_Q2",
      "identificacion_metodo": "contenido",
      "columnas_muestra": ["ID_VIV", "ID_PER", "UPM", "VIV_SEL", "R_SEL"],
      "total_columnas": 145
    }
  ]
}
```

---

## Fase 2: Validación

### Objetivo
Validar que cada archivo encontrado sea procesable, verificando columnas requeridas y valores válidos.

### Script: `fase2_validacion.py`

#### Entradas
- `temp/mapeo_archivos.json`: Salida de Fase 1
- Archivos CSV identificados en el mapeo

#### Salidas
- `temp/archivos_validados.json`: Lista de archivos procesables con sus validaciones
- `logs/fase2_validacion_YYYYMMDD_HHMMSS.log`: Log detallado de validación

#### Código del Script

**IMPORTANTE**: Guarda el siguiente código en el archivo `scripts/fase2_validacion.py`

```python
#!/usr/bin/env python3
"""
Fase 2: Validación de archivos ENSU
"""

import json
import logging
import pandas as pd
from datetime import datetime
import os

def setup_logging():
    """Configurar sistema de logging"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = f"logs/fase2_validacion_{timestamp}.log"
    
    os.makedirs("logs", exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] %(levelname)s: %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def load_mapping(filepath="temp/mapeo_archivos.json"):
    """Cargar mapeo de archivos de Fase 1"""
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)

def validate_columns(filepath, required_columns):
    """Validar que el archivo tenga las columnas requeridas"""
    try:
        # Leer 5 filas para validación (estandarizado)
        df = pd.read_csv(filepath, nrows=5)
        columnas_presentes = set(df.columns)
        columnas_requeridas = set(required_columns)
        
        columnas_faltantes = columnas_requeridas - columnas_presentes
        
        return {
            'columnas_presentes': True if not columnas_faltantes else False,
            'columnas_faltantes': list(columnas_faltantes),
            'columnas_totales': len(df.columns)
        }
    except Exception as e:
        return {
            'columnas_presentes': False,
            'error': str(e)
        }

def validate_bp1_1_values(filepath, sample_size=1000):
    """Validar valores en columna BP1_1"""
    try:
        df = pd.read_csv(filepath, usecols=['BP1_1'], nrows=sample_size)
        
        # Convertir a numérico
        df['BP1_1'] = pd.to_numeric(df['BP1_1'], errors='coerce')
        
        # Valores únicos
        valores_unicos = df['BP1_1'].dropna().unique()
        valores_validos = [1, 2, 9]
        valores_invalidos = [v for v in valores_unicos if v not in valores_validos]
        
        # Conteo de valores
        value_counts = df['BP1_1'].value_counts().to_dict()
        
        # Calcular NULLs
        total_rows = len(df)
        null_count = df['BP1_1'].isna().sum()
        null_percentage = (null_count / total_rows * 100) if total_rows > 0 else 0
        
        return {
            'valores_validos': len(valores_invalidos) == 0,
            'valores_unicos': sorted(valores_unicos.tolist()),
            'valores_invalidos': valores_invalidos,
            'distribucion': value_counts,
            'nulls_count': int(null_count),
            'nulls_percentage': round(null_percentage, 2),
            'muestra_size': total_rows
        }
    except Exception as e:
        return {
            'valores_validos': False,
            'error': str(e)
        }

def check_yucatan_data(filepath):
    """Verificar si hay datos de Yucatán"""
    try:
        df = pd.read_csv(filepath, usecols=['NOM_ENT'])
        
        # Buscar Yucatán (con y sin tilde)
        yucatan_mask = df['NOM_ENT'].str.upper().isin(['YUCATAN', 'YUCATÁN'])
        yucatan_count = yucatan_mask.sum()
        
        # Estados únicos
        estados_unicos = df['NOM_ENT'].nunique()
        
        return {
            'tiene_yucatan': yucatan_count > 0,
            'registros_yucatan': int(yucatan_count),
            'total_registros': len(df),
            'porcentaje_yucatan': round((yucatan_count / len(df) * 100), 2) if len(df) > 0 else 0,
            'estados_unicos': int(estados_unicos)
        }
    except Exception as e:
        return {
            'tiene_yucatan': False,
            'error': str(e)
        }

def validate_file(archivo_metadata):
    """Validación completa de un archivo"""
    logger = logging.getLogger(__name__)
    filepath = archivo_metadata['filepath']
    
    logger.info(f"Validando: {filepath}")
    
    # Columnas requeridas
    required_columns = ['NOM_ENT', 'NOM_MUN', 'NOM_CD', 'BP1_1']
    
    # Realizar validaciones
    validacion = {
        'filepath': filepath,
        'periodo_str': archivo_metadata['periodo_str'],
        'validacion_columnas': validate_columns(filepath, required_columns),
        'validacion_bp1_1': validate_bp1_1_values(filepath),
        'validacion_yucatan': check_yucatan_data(filepath)
    }
    
    # Determinar si es procesable
    es_procesable = (
        validacion['validacion_columnas'].get('columnas_presentes', False) and
        validacion['validacion_bp1_1'].get('valores_validos', False) and
        validacion['validacion_yucatan'].get('tiene_yucatan', False)
    )
    
    validacion['es_procesable'] = es_procesable
    
    # Log de resultado
    if es_procesable:
        logger.info(f"  ✓ PROCESABLE - {archivo_metadata['periodo_str']}")
    else:
        logger.warning(f"  ✗ NO PROCESABLE - {archivo_metadata['periodo_str']}")
        if not validacion['validacion_columnas'].get('columnas_presentes', False):
            logger.warning(f"    Columnas faltantes: {validacion['validacion_columnas'].get('columnas_faltantes', [])}")
        if not validacion['validacion_bp1_1'].get('valores_validos', False):
            logger.warning(f"    Valores inválidos en BP1_1: {validacion['validacion_bp1_1'].get('valores_invalidos', [])}")
        if not validacion['validacion_yucatan'].get('tiene_yucatan', False):
            logger.warning(f"    No contiene datos de Yucatán")
    
    return validacion

def detect_and_resolve_duplicates(validaciones):
    """Detectar y resolver archivos duplicados por período"""
    logger = logging.getLogger(__name__)
    
    periodos = {}
    for val in validaciones:
        if val['es_procesable']:
            periodo = val['periodo_str']
            if periodo not in periodos:
                periodos[periodo] = []
            periodos[periodo].append(val)
    
    duplicados_info = {}
    archivos_seleccionados = {}
    
    for periodo, vals in periodos.items():
        if len(vals) > 1:
            logger.warning(f"=== DUPLICADOS DETECTADOS PARA {periodo} ===")
            logger.warning(f"  Se encontraron {len(vals)} archivos:")
            
            # Analizar cada archivo duplicado
            for val in vals:
                filepath = val['filepath']
                try:
                    file_stats = os.stat(filepath)
                    size_mb = file_stats.st_size / (1024 * 1024)
                    mod_time = datetime.fromtimestamp(file_stats.st_mtime)
                    
                    # Contar registros de Yucatán
                    yuc_count = val['validacion_yucatan'].get('registros_yucatan', 0)
                    
                    logger.warning(f"    - {filepath}")
                    logger.warning(f"      Tamaño: {size_mb:.2f} MB")
                    logger.warning(f"      Modificado: {mod_time.isoformat()}")
                    logger.warning(f"      Registros Yucatán: {yuc_count}")
                    
                    val['_file_size_mb'] = size_mb
                    val['_modified_time'] = mod_time
                    val['_yucatan_count'] = yuc_count
                except Exception as e:
                    logger.error(f"      Error obteniendo stats: {e}")
                    val['_file_size_mb'] = 0
                    val['_modified_time'] = datetime.min
                    val['_yucatan_count'] = 0
            
            # Seleccionar el mejor archivo (más reciente con más datos de Yucatán)
            mejor_archivo = max(vals, key=lambda x: (x['_yucatan_count'], x['_modified_time']))
            archivos_seleccionados[periodo] = mejor_archivo['filepath']
            
            logger.info(f"  ✓ Archivo seleccionado: {mejor_archivo['filepath']}")
            logger.info(f"    Razón: {mejor_archivo['_yucatan_count']} registros de Yucatán, modificado {mejor_archivo['_modified_time'].isoformat()}")
            
            # Marcar los otros como duplicados
            for val in vals:
                if val['filepath'] != mejor_archivo['filepath']:
                    val['es_procesable'] = False
                    val['razon_no_procesable'] = f'Duplicado - Se seleccionó {os.path.basename(mejor_archivo["filepath"])}'
            
            duplicados_info[periodo] = {
                'archivos': [v['filepath'] for v in vals],
                'seleccionado': mejor_archivo['filepath'],
                'criterio': f'{mejor_archivo["_yucatan_count"]} registros Yucatán'
            }
    
    return duplicados_info, archivos_seleccionados

def save_validation(validaciones, output_path="temp/archivos_validados.json"):
    """Guardar resultados de validación"""
    logger = logging.getLogger(__name__)
    
    # Asegurar que el directorio temp existe
    os.makedirs("temp", exist_ok=True)
    
    # Detectar y resolver duplicados
    duplicados_info, archivos_seleccionados = detect_and_resolve_duplicates(validaciones)
    
    output_data = {
        'timestamp': datetime.now().isoformat(),
        'total_archivos': len(validaciones),
        'procesables': sum(1 for v in validaciones if v['es_procesable']),
        'no_procesables': sum(1 for v in validaciones if not v['es_procesable']),
        'duplicados_info': duplicados_info,
        'archivos_seleccionados': archivos_seleccionados,
        'validaciones': validaciones
    }
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    
    logger.info(f"=== RESUMEN DE VALIDACIÓN ===")
    logger.info(f"Total archivos analizados: {output_data['total_archivos']}")
    logger.info(f"Archivos procesables: {output_data['procesables']}")
    logger.info(f"Archivos no procesables: {output_data['no_procesables']}")
    logger.info(f"Períodos con duplicados resueltos: {len(duplicados_info)}")
    
    if duplicados_info:
        logger.info("Duplicados resueltos:")
        for periodo, info in duplicados_info.items():
            logger.info(f"  {periodo}: {len(info['archivos'])} archivos, seleccionado por {info['criterio']}")

def main():
    logger = setup_logging()
    
    try:
        logger.info("=== INICIANDO FASE 2: VALIDACIÓN ===")
        
        # Cargar mapeo de Fase 1
        mapeo = load_mapping()
        
        # Validar cada archivo
        validaciones = []
        for archivo in mapeo['archivos']:
            validacion = validate_file(archivo)
            validaciones.append(validacion)
        
        # Guardar resultados
        save_validation(validaciones)
        
        logger.info("=== FASE 2 COMPLETADA EXITOSAMENTE ===")
        
    except Exception as e:
        logger.error(f"Error fatal en Fase 2: {e}")
        raise

if __name__ == "__main__":
    main()
```

#### Formato de Salida: `archivos_validados.json`
```json
{
  "timestamp": "2024-09-04T14:35:00",
  "total_archivos": 3,
  "procesables": 2,
  "no_procesables": 1,
  "duplicados_info": {},
  "archivos_seleccionados": {},
  "validaciones": [
    {
      "filepath": "data/conjunto_de_datos_ensu_2025_2t_csv/...",
      "periodo_str": "2025_Q2",
      "validacion_columnas": {
        "columnas_presentes": true,
        "columnas_faltantes": [],
        "columnas_totales": 145
      },
      "validacion_bp1_1": {
        "valores_validos": true,
        "valores_unicos": [1, 2],
        "valores_invalidos": [],
        "distribucion": {"1": 450, "2": 550},
        "nulls_count": 0,
        "nulls_percentage": 0.0,
        "muestra_size": 1000
      },
      "validacion_yucatan": {
        "tiene_yucatan": true,
        "registros_yucatan": 255,
        "total_registros": 23717,
        "porcentaje_yucatan": 1.08,
        "estados_unicos": 32
      },
      "es_procesable": true
    }
  ]
}
```

---

## Fase 3: Verificación Incremental

### Objetivo
Verificar qué archivos ya han sido procesados para evitar reprocesamiento.

### Script: `fase3_verificacion_incremental.py`

#### Entradas
- `temp/archivos_validados.json`: Salida de Fase 2
- Directorio: `data/yucatan_processed/`

#### Salidas
- `temp/archivos_pendientes.json`: Lista de archivos a procesar
- `logs/fase3_verificacion_YYYYMMDD_HHMMSS.log`: Log de verificación

#### Código del Script

**IMPORTANTE**: Guarda el siguiente código en el archivo `scripts/fase3_verificacion_incremental.py`

```python
#!/usr/bin/env python3
"""
Fase 3: Verificación Incremental
"""

import json
import logging
import os
from datetime import datetime
from pathlib import Path

def setup_logging():
    """Configurar sistema de logging"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = f"logs/fase3_verificacion_{timestamp}.log"
    
    os.makedirs("logs", exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] %(levelname)s: %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def load_validated_files(filepath="temp/archivos_validados.json"):
    """Cargar archivos validados de Fase 2"""
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)

def scan_processed_files(processed_dir="data/yucatan_processed"):
    """Escanear archivos ya procesados"""
    logger = logging.getLogger(__name__)
    
    processed_files = {}
    
    if not os.path.exists(processed_dir):
        logger.info(f"Directorio {processed_dir} no existe. Se asume que no hay archivos procesados.")
        return processed_files
    
    for file in os.listdir(processed_dir):
        if file.endswith('.csv'):
            # Extraer período del nombre del archivo
            # Formato esperado: yucatan_security_YYYY_QN.csv
            parts = file.replace('.csv', '').split('_')
            if len(parts) >= 4:
                try:
                    year = parts[-2]
                    quarter = parts[-1]
                    periodo_str = f"{year}_{quarter}"
                    
                    file_path = os.path.join(processed_dir, file)
                    file_stats = os.stat(file_path)
                    
                    processed_files[periodo_str] = {
                        'filename': file,
                        'filepath': file_path,
                        'size_mb': round(file_stats.st_size / (1024 * 1024), 2),
                        'modified_date': datetime.fromtimestamp(file_stats.st_mtime).isoformat()
                    }
                    
                    logger.info(f"Archivo procesado encontrado: {file} ({periodo_str})")
                except Exception as e:
                    logger.warning(f"No se pudo parsear período de {file}: {e}")
    
    return processed_files

def identify_pending_files(validated_data, processed_files):
    """Identificar archivos pendientes de procesar"""
    logger = logging.getLogger(__name__)
    
    pending = []
    skipped = []
    
    for validacion in validated_data['validaciones']:
        if not validacion['es_procesable']:
            # Los archivos con período UNKNOWN_XXX no se procesan automáticamente
            # Requieren revisión manual para identificar el período correcto
            if 'UNKNOWN' in validacion.get('periodo_str', ''):
                logger.warning(f"SALTANDO archivo con período no identificado: {validacion['periodo_str']}")
            continue
        
        periodo = validacion['periodo_str']
        
        if periodo in processed_files:
            logger.info(f"SALTANDO {periodo}: Ya procesado ({processed_files[periodo]['filename']})")
            skipped.append({
                'periodo': periodo,
                'archivo_original': validacion['filepath'],
                'archivo_procesado': processed_files[periodo]['filepath'],
                'fecha_procesamiento': processed_files[periodo]['modified_date']
            })
        else:
            logger.info(f"PENDIENTE {periodo}: Será procesado")
            pending.append(validacion)
    
    return pending, skipped

def save_pending_files(pending, skipped, output_path="temp/archivos_pendientes.json"):
    """Guardar lista de archivos pendientes"""
    logger = logging.getLogger(__name__)
    
    # Asegurar que el directorio temp existe
    os.makedirs("temp", exist_ok=True)
    
    output_data = {
        'timestamp': datetime.now().isoformat(),
        'total_pendientes': len(pending),
        'total_saltados': len(skipped),
        'archivos_pendientes': pending,
        'archivos_saltados': skipped
    }
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    
    logger.info(f"=== RESUMEN DE VERIFICACIÓN INCREMENTAL ===")
    logger.info(f"Archivos pendientes de procesar: {len(pending)}")
    logger.info(f"Archivos ya procesados (saltados): {len(skipped)}")
    
    if pending:
        logger.info("Períodos a procesar:")
        for p in pending:
            logger.info(f"  - {p['periodo_str']}")

def main():
    logger = setup_logging()
    
    try:
        logger.info("=== INICIANDO FASE 3: VERIFICACIÓN INCREMENTAL ===")
        
        # Cargar archivos validados
        validated_data = load_validated_files()
        
        # Escanear archivos ya procesados
        processed_files = scan_processed_files()
        
        # Identificar pendientes
        pending, skipped = identify_pending_files(validated_data, processed_files)
        
        # Guardar resultados
        save_pending_files(pending, skipped)
        
        logger.info("=== FASE 3 COMPLETADA EXITOSAMENTE ===")
        
    except Exception as e:
        logger.error(f"Error fatal en Fase 3: {e}")
        raise

if __name__ == "__main__":
    main()
```

---

## Fase 4: Procesamiento

### Objetivo
Procesar los archivos pendientes: filtrar por Yucatán, agregar datos y calcular estadísticas.

### Script: `fase4_procesamiento.py`

#### Entradas
- `temp/archivos_pendientes.json`: Salida de Fase 3
- Archivos CSV originales

#### Salidas
- `data/yucatan_processed/yucatan_security_YYYY_QN.csv`: Archivos procesados
- `temp/procesamiento_resultados.json`: Resumen del procesamiento
- `logs/fase4_procesamiento_YYYYMMDD_HHMMSS.log`: Log de procesamiento

#### Código del Script

**IMPORTANTE**: Guarda el siguiente código en el archivo `scripts/fase4_procesamiento.py`

```python
#!/usr/bin/env python3
"""
Fase 4: Procesamiento de datos ENSU para Yucatán
"""

import json
import logging
import pandas as pd
import os
from datetime import datetime

def setup_logging():
    """Configurar sistema de logging"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = f"logs/fase4_procesamiento_{timestamp}.log"
    
    os.makedirs("logs", exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] %(levelname)s: %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def load_pending_files(filepath="temp/archivos_pendientes.json"):
    """Cargar archivos pendientes de Fase 3"""
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)

def process_single_file(file_info):
    """Procesar un archivo individual"""
    logger = logging.getLogger(__name__)
    filepath = file_info['filepath']
    periodo = file_info['periodo_str']
    
    logger.info(f"{'='*60}")
    logger.info(f"PROCESANDO ARCHIVO: {os.path.basename(filepath)}")
    logger.info(f"Período: {periodo}")
    logger.info(f"Ruta completa: {filepath}")
    
    try:
        # Verificar que el archivo existe
        if not os.path.exists(filepath):
            logger.error(f"ERROR: El archivo no existe en la ruta especificada")
            return None
            
        file_size_mb = os.path.getsize(filepath) / (1024 * 1024)
        logger.info(f"Tamaño del archivo: {file_size_mb:.2f} MB")
        
        # Leer archivo completo
        logger.info("Leyendo archivo CSV...")
        df = pd.read_csv(filepath)
        logger.info(f"✓ Archivo leído exitosamente")
        logger.info(f"  - Registros totales: {len(df):,}")
        logger.info(f"  - Columnas totales: {len(df.columns)}")
        
        # Verificar columnas requeridas
        columns_required = ['NOM_ENT', 'NOM_MUN', 'NOM_CD', 'BP1_1']
        missing_cols = [col for col in columns_required if col not in df.columns]
        if missing_cols:
            logger.error(f"ERROR: Columnas faltantes: {missing_cols}")
            return None
        
        logger.info(f"✓ Todas las columnas requeridas presentes")
        
        # Seleccionar columnas y analizar datos iniciales
        df = df[columns_required].copy()
        logger.info(f"Analizando datos iniciales:")
        logger.info(f"  - Valores únicos en NOM_ENT: {df['NOM_ENT'].nunique()}")
        logger.info(f"  - Estados presentes: {', '.join(df['NOM_ENT'].unique()[:5])}...")
        
        # Analizar BP1_1 antes de conversión
        logger.info("Analizando columna BP1_1:")
        logger.info(f"  - Tipo de datos original: {df['BP1_1'].dtype}")
        logger.info(f"  - Valores únicos (muestra): {df['BP1_1'].value_counts().head().to_dict()}")
        logger.info(f"  - Valores nulos: {df['BP1_1'].isna().sum():,}")
        
        # Convertir BP1_1 a numérico
        df['BP1_1'] = pd.to_numeric(df['BP1_1'], errors='coerce')
        nulls_after_conversion = df['BP1_1'].isna().sum()
        logger.info(f"Conversión a numérico completada:")
        logger.info(f"  - Nuevos nulos después de conversión: {nulls_after_conversion:,}")
        
        # Analizar valores antes del filtrado
        all_values = df['BP1_1'].dropna().unique()
        valid_values = [1, 2, 9]
        invalid_values = [v for v in all_values if v not in valid_values]
        
        if invalid_values:
            logger.warning(f"⚠ Valores no estándar encontrados en BP1_1: {invalid_values}")
            value_dist = df['BP1_1'].value_counts().to_dict()
            for val in invalid_values:
                count = value_dist.get(val, 0)
                logger.warning(f"    Valor {val}: {count:,} registros ({count/len(df)*100:.2f}%)")
        
        # Filtrar solo valores válidos (1, 2, 9)
        valid_mask = df['BP1_1'].isin(valid_values)
        registros_invalidos = (~valid_mask).sum()
        df = df[valid_mask].copy()
        
        logger.info(f"Filtrado por valores válidos de BP1_1:")
        logger.info(f"  - Registros con valores válidos: {len(df):,}")
        logger.info(f"  - Registros descartados: {registros_invalidos:,}")
        logger.info(f"  - Distribución de valores válidos:")
        for val in valid_values:
            count = (df['BP1_1'] == val).sum()
            if count > 0:
                logger.info(f"    BP1_1={val}: {count:,} ({count/len(df)*100:.1f}%)")
        
        # Filtrar por Yucatán
        # IMPORTANTE: El formato de NOM_ENT varía según el año:
        # - 2016-2017: 'Yucatán\r' (con tilde y retorno de carro)
        # - 2018-2021: 'Yucatan\r' o 'Yucatan' (sin tilde, con o sin \r)
        # - 2022+: 'YUCATAN' (mayúsculas sin tilde)
        logger.info("Filtrando registros de Yucatán...")
        # Limpiar espacios, caracteres de retorno y normalizar
        df['NOM_ENT_CLEAN'] = df['NOM_ENT'].str.strip().str.upper()
        df_yucatan = df[df['NOM_ENT_CLEAN'].str.contains('YUCAT', case=False, na=False)].copy()
        
        logger.info(f"Resultados del filtrado por Yucatán:")
        logger.info(f"  - Registros de Yucatán: {len(df_yucatan):,}")
        logger.info(f"  - Porcentaje del total: {len(df_yucatan)/len(df)*100:.2f}%")
        
        if len(df_yucatan) == 0:
            logger.error(f"ERROR: No se encontraron registros de Yucatán")
            return None
        elif len(df_yucatan) < 10:
            logger.warning(f"⚠ ADVERTENCIA: Muy pocos registros de Yucatán ({len(df_yucatan)})")
            logger.warning(f"  Los resultados pueden no ser estadísticamente significativos")
        
        # Análisis detallado de Yucatán
        logger.info(f"Análisis de datos de Yucatán:")
        logger.info(f"  - Municipios únicos: {df_yucatan['NOM_MUN'].nunique()}")
        logger.info(f"  - Ciudades únicas: {df_yucatan['NOM_CD'].nunique()}")
        
        # Distribución de BP1_1 en Yucatán
        logger.info(f"  - Distribución de percepción en Yucatán:")
        for val, label in [(1, 'Seguro'), (2, 'Inseguro'), (9, 'No responde')]:
            count = (df_yucatan['BP1_1'] == val).sum()
            if count > 0:
                logger.info(f"    {label} (BP1_1={val}): {count:,} ({count/len(df_yucatan)*100:.1f}%)")
        
        # Agrupar y calcular estadísticas
        logger.info("Agrupando datos por municipio y ciudad...")
        grouped_data = []
        
        for (nom_ent, nom_mun, nom_cd), group in df_yucatan.groupby(['NOM_ENT', 'NOM_MUN', 'NOM_CD']):
            total_seguros = (group['BP1_1'] == 1).sum()
            total_inseguros = (group['BP1_1'] == 2).sum()
            total_no_responde = (group['BP1_1'] == 9).sum()
            total_respuestas = len(group)
            
            # Log para ciudades con pocos registros
            if total_respuestas < 5:
                logger.warning(f"  ⚠ {nom_mun} - {nom_cd}: Solo {total_respuestas} respuestas")
            
            grouped_data.append({
                'NOM_ENT': nom_ent,
                'NOM_MUN': nom_mun,
                'NOM_CD': nom_cd,
                'TOTAL_SEGUROS': int(total_seguros),
                'TOTAL_INSEGUROS': int(total_inseguros),
                'TOTAL_NO_RESPONDE': int(total_no_responde),
                'TOTAL_RESPUESTAS': int(total_respuestas)
            })
        
        df_resultado = pd.DataFrame(grouped_data)
        logger.info(f"✓ Agrupación completada: {len(df_resultado)} combinaciones municipio-ciudad")
        
        # Calcular porcentajes
        df_resultado['PORCENTAJE_SEGUROS'] = (
            df_resultado['TOTAL_SEGUROS'] / df_resultado['TOTAL_RESPUESTAS'] * 100
        ).round(2)
        df_resultado['PORCENTAJE_INSEGUROS'] = (
            df_resultado['TOTAL_INSEGUROS'] / df_resultado['TOTAL_RESPUESTAS'] * 100
        ).round(2)
        df_resultado['PORCENTAJE_NO_RESPONDE'] = (
            df_resultado['TOTAL_NO_RESPONDE'] / df_resultado['TOTAL_RESPUESTAS'] * 100
        ).round(2)
        
        # Agregar metadatos
        df_resultado['PERIODO'] = periodo
        df_resultado['FECHA_PROCESAMIENTO'] = datetime.now().isoformat()
        
        # Log de resumen por municipio
        logger.info("Resumen por municipio:")
        mun_summary = df_resultado.groupby('NOM_MUN').agg({
            'TOTAL_RESPUESTAS': 'sum',
            'TOTAL_SEGUROS': 'sum',
            'TOTAL_INSEGUROS': 'sum'
        })
        
        for mun, row in mun_summary.iterrows():
            pct_seg = row['TOTAL_SEGUROS'] / row['TOTAL_RESPUESTAS'] * 100
            logger.info(f"  - {mun}: {row['TOTAL_RESPUESTAS']:,} respuestas, {pct_seg:.1f}% seguro")
        
        logger.info(f"Totales finales del procesamiento:")
        logger.info(f"  - Municipios únicos: {df_resultado['NOM_MUN'].nunique()}")
        logger.info(f"  - Ciudades únicas: {df_resultado['NOM_CD'].nunique()}")
        logger.info(f"  - Total de registros agregados: {len(df_resultado)}")
        
        # Estadísticas generales
        total_general = df_resultado['TOTAL_RESPUESTAS'].sum()
        pct_seguros = (df_resultado['TOTAL_SEGUROS'].sum() / total_general * 100)
        pct_inseguros = (df_resultado['TOTAL_INSEGUROS'].sum() / total_general * 100)
        
        logger.info(f"  Percepción general - Seguros: {pct_seguros:.1f}%, Inseguros: {pct_inseguros:.1f}%")
        
        return {
            'dataframe': df_resultado,
            'estadisticas': {
                'periodo': periodo,
                'registros_originales': len(df),
                'registros_yucatan': len(df_yucatan),
                'municipios': int(df_resultado['NOM_MUN'].nunique()),
                'ciudades': int(df_resultado['NOM_CD'].nunique()),
                'total_respuestas': int(total_general),
                'porcentaje_seguros_general': round(pct_seguros, 2),
                'porcentaje_inseguros_general': round(pct_inseguros, 2)
            }
        }
        
    except Exception as e:
        logger.error(f"  Error procesando archivo: {e}")
        return None

def save_processed_file(result, periodo, output_dir="data/yucatan_processed"):
    """Guardar archivo procesado"""
    logger = logging.getLogger(__name__)
    
    os.makedirs(output_dir, exist_ok=True)
    
    # Generar nombre de archivo
    output_filename = f"yucatan_security_{periodo}.csv"
    output_path = os.path.join(output_dir, output_filename)
    
    # Guardar DataFrame
    result['dataframe'].to_csv(output_path, index=False, encoding='utf-8')
    
    logger.info(f"  Archivo guardado: {output_path}")
    
    return output_path

def save_processing_summary(results, output_path="temp/procesamiento_resultados.json"):
    """Guardar resumen del procesamiento"""
    logger = logging.getLogger(__name__)
    
    # Asegurar que el directorio temp existe
    os.makedirs("temp", exist_ok=True)
    
    summary = {
        'timestamp': datetime.now().isoformat(),
        'archivos_procesados': len([r for r in results if r['exito']]),
        'archivos_fallidos': len([r for r in results if not r['exito']]),
        'resultados': results
    }
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    
    logger.info(f"=== RESUMEN DE PROCESAMIENTO ===")
    logger.info(f"Archivos procesados exitosamente: {summary['archivos_procesados']}")
    logger.info(f"Archivos con errores: {summary['archivos_fallidos']}")

def main():
    logger = setup_logging()
    
    try:
        logger.info("=== INICIANDO FASE 4: PROCESAMIENTO ===")
        
        # Cargar archivos pendientes
        pending_data = load_pending_files()
        
        if pending_data['total_pendientes'] == 0:
            logger.info("No hay archivos pendientes de procesar")
            return
        
        # Procesar cada archivo
        results = []
        for file_info in pending_data['archivos_pendientes']:
            logger.info(f"\n--- Procesando {file_info['periodo_str']} ---")
            
            result = process_single_file(file_info)
            
            if result:
                # Guardar archivo procesado
                output_path = save_processed_file(
                    result, 
                    file_info['periodo_str']
                )
                
                results.append({
                    'periodo': file_info['periodo_str'],
                    'archivo_origen': file_info['filepath'],
                    'archivo_salida': output_path,
                    'exito': True,
                    'estadisticas': result['estadisticas']
                })
            else:
                results.append({
                    'periodo': file_info['periodo_str'],
                    'archivo_origen': file_info['filepath'],
                    'exito': False,
                    'error': 'Error en procesamiento'
                })
        
        # Guardar resumen
        save_processing_summary(results)
        
        logger.info("\n=== FASE 4 COMPLETADA EXITOSAMENTE ===")
        
    except Exception as e:
        logger.error(f"Error fatal en Fase 4: {e}")
        raise

if __name__ == "__main__":
    main()
```

#### Formato de Salida: `yucatan_security_YYYY_QN.csv`
```csv
NOM_ENT,NOM_MUN,NOM_CD,TOTAL_SEGUROS,TOTAL_INSEGUROS,TOTAL_NO_RESPONDE,TOTAL_RESPUESTAS,PORCENTAJE_SEGUROS,PORCENTAJE_INSEGUROS,PORCENTAJE_NO_RESPONDE,PERIODO,FECHA_PROCESAMIENTO
YUCATÁN,MÉRIDA,MÉRIDA,120,80,5,205,58.54,39.02,2.44,2025_Q2,2024-09-04T15:00:00
YUCATÁN,PROGRESO,PROGRESO,45,25,2,72,62.50,34.72,2.78,2025_Q2,2024-09-04T15:00:00
```

---

## Fase 5: Reporte Final

### Objetivo
Generar reportes consolidados y análisis de tendencias.

### Script: `fase5_reporte.py`

#### Entradas
- `data/yucatan_processed/*.csv`: Todos los archivos procesados
- `temp/resumen_procesamiento.json`: Resumen de la Fase 4

#### Salidas
- `reports/dataset_maestro_yucatan.csv`: Dataset consolidado con todos los registros
- `reports/reporte_ensu_yucatan.html`: Reporte HTML visual interactivo
- `reports/estadisticas_finales.json`: Estadísticas completas en JSON
- `logs/fase5_reporte.log`: Log de generación de reporte

#### Código del Script

**IMPORTANTE**: Guarda el siguiente código en el archivo `scripts/fase5_reporte.py`

```python
#!/usr/bin/env python3
"""
Fase 5: Reporte Final
Genera un reporte completo del procesamiento y un dataset maestro
"""

import json
import pandas as pd
import logging
from pathlib import Path
from datetime import datetime
import os
from glob import glob

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/fase5_reporte.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def cargar_datasets_procesados():
    """
    Carga todos los datasets procesados de Yucatán
    """
    logger.info("Cargando datasets procesados...")
    archivos_csv = glob('data/yucatan_processed/*.csv')
    
    datasets = []
    for archivo in sorted(archivos_csv):
        logger.info(f"  - Cargando: {os.path.basename(archivo)}")
        df = pd.read_csv(archivo)
        datasets.append(df)
    
    if datasets:
        df_completo = pd.concat(datasets, ignore_index=True)
        logger.info(f"Total de registros combinados: {len(df_completo)}")
        return df_completo
    else:
        logger.warning("No se encontraron datasets procesados")
        return pd.DataFrame()

def generar_estadisticas_globales(df):
    """
    Genera estadísticas globales del dataset completo
    """
    if df.empty:
        return {}
    
    estadisticas = {
        'total_registros': int(len(df)),
        'años_cubiertos': [int(x) for x in sorted(df['AÑO'].unique().tolist())],
        'trimestres_totales': int(len(df[['AÑO', 'TRIMESTRE']].drop_duplicates())),
        'municipios_unicos': int(df['NOM_MUN'].nunique()),
        'ciudades_unicas': int(df['NOM_CD'].nunique()),
        
        # Estadísticas de percepción
        'total_respuestas_seguro': int(df['TOTAL_SEGUROS'].sum()),
        'total_respuestas_inseguro': int(df['TOTAL_INSEGUROS'].sum()),
        'total_no_responde': int(df['TOTAL_NO_RESPONDE'].sum()),
        
        # Promedios generales
        'promedio_pct_seguros': round(df['PCT_SEGUROS'].mean(), 2),
        'promedio_pct_inseguros': round(df['PCT_INSEGUROS'].mean(), 2),
        
        # Por año
        'estadisticas_por_año': []
    }
    
    # Calcular estadísticas por año
    for año in sorted(df['AÑO'].unique()):
        df_año = df[df['AÑO'] == año]
        estadisticas['estadisticas_por_año'].append({
            'año': int(año),
            'trimestres': int(len(df_año['TRIMESTRE'].unique())),
            'registros': int(len(df_año)),
            'promedio_pct_seguros': round(df_año['PCT_SEGUROS'].mean(), 2),
            'promedio_pct_inseguros': round(df_año['PCT_INSEGUROS'].mean(), 2)
        })
    
    # Municipios más seguros/inseguros
    municipios_stats = df.groupby('NOM_MUN').agg({
        'PCT_SEGUROS': 'mean',
        'PCT_INSEGUROS': 'mean',
        'TOTAL_REGISTROS': 'sum'
    }).round(2)
    
    estadisticas['municipio_mas_seguro'] = {
        'nombre': municipios_stats['PCT_SEGUROS'].idxmax(),
        'pct_promedio_seguros': float(municipios_stats['PCT_SEGUROS'].max())
    }
    
    estadisticas['municipio_mas_inseguro'] = {
        'nombre': municipios_stats['PCT_INSEGUROS'].idxmax(),
        'pct_promedio_inseguros': float(municipios_stats['PCT_INSEGUROS'].max())
    }
    
    return estadisticas

def generar_reporte_html(estadisticas, resumen_procesamiento):
    """
    Genera un reporte HTML con todas las estadísticas
    """
    html_content = """
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Reporte ENSU Yucatán - Percepción de Seguridad</title>
    <style>
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 20px;
            background-color: #f5f5f5;
        }
        .container {
            max-width: 1200px;
            margin: 0 auto;
            background: white;
            padding: 30px;
            border-radius: 10px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }
        h1 {
            color: #2c3e50;
            border-bottom: 3px solid #3498db;
            padding-bottom: 10px;
        }
        h2 {
            color: #34495e;
            margin-top: 30px;
        }
        .stat-card {
            background: #ecf0f1;
            padding: 15px;
            border-radius: 5px;
            margin: 10px 0;
        }
        .stat-label {
            font-weight: bold;
            color: #7f8c8d;
        }
        .stat-value {
            font-size: 24px;
            color: #2c3e50;
            margin-top: 5px;
        }
        .grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin: 20px 0;
        }
        table {
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
        }
        th {
            background-color: #3498db;
            color: white;
            padding: 10px;
            text-align: left;
        }
        td {
            padding: 10px;
            border-bottom: 1px solid #ecf0f1;
        }
        tr:hover {
            background-color: #f8f9fa;
        }
        .success {
            color: #27ae60;
        }
        .warning {
            color: #f39c12;
        }
        .info {
            color: #3498db;
        }
        .footer {
            margin-top: 40px;
            padding-top: 20px;
            border-top: 1px solid #ecf0f1;
            text-align: center;
            color: #7f8c8d;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>📊 Reporte ENSU Yucatán - Percepción de Seguridad</h1>
        <p>Fecha de generación: {{FECHA_GENERACION}}</p>
        
        <h2>📈 Resumen del Procesamiento</h2>
        <div class="grid">
            <div class="stat-card">
                <div class="stat-label">Archivos Procesados</div>
                <div class="stat-value success">{{ARCHIVOS_EXITOSOS}}/{{TOTAL_ARCHIVOS}}</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Total Registros Yucatán</div>
                <div class="stat-value info">{{TOTAL_REGISTROS}}</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Años Cubiertos</div>
                <div class="stat-value">{{AÑOS_CUBIERTOS}}</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Trimestres Procesados</div>
                <div class="stat-value">{{TRIMESTRES_TOTALES}}</div>
            </div>
        </div>
        
        <h2>🔍 Percepción General de Seguridad</h2>
        <div class="grid">
            <div class="stat-card">
                <div class="stat-label">Promedio % Seguro</div>
                <div class="stat-value success">{{PROMEDIO_PCT_SEGUROS}}%</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Promedio % Inseguro</div>
                <div class="stat-value warning">{{PROMEDIO_PCT_INSEGUROS}}%</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Municipios Analizados</div>
                <div class="stat-value">{{MUNICIPIOS_UNICOS}}</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Ciudades Analizadas</div>
                <div class="stat-value">{{CIUDADES_UNICAS}}</div>
            </div>
        </div>
        
        <h2>📅 Evolución por Año</h2>
        <table>
            <thead>
                <tr>
                    <th>Año</th>
                    <th>Trimestres</th>
                    <th>Registros</th>
                    <th>% Promedio Seguro</th>
                    <th>% Promedio Inseguro</th>
                </tr>
            </thead>
            <tbody>
                {{TABLA_AÑOS}}
            </tbody>
        </table>
        
        <h2>🏆 Rankings de Municipios</h2>
        <div class="grid">
            <div class="stat-card">
                <div class="stat-label">Municipio Más Seguro</div>
                <div class="stat-value success">{{MUNICIPIO_MAS_SEGURO}}</div>
                <small>{{PCT_MAS_SEGURO}}% se sienten seguros</small>
            </div>
            <div class="stat-card">
                <div class="stat-label">Municipio Más Inseguro</div>
                <div class="stat-value warning">{{MUNICIPIO_MAS_INSEGURO}}</div>
                <small>{{PCT_MAS_INSEGURO}}% se sienten inseguros</small>
            </div>
        </div>
        
        <h2>📁 Archivos Procesados</h2>
        <table>
            <thead>
                <tr>
                    <th>Archivo Original</th>
                    <th>Año</th>
                    <th>Trimestre</th>
                    <th>Registros Yucatán</th>
                    <th>Municipios</th>
                </tr>
            </thead>
            <tbody>
                {{TABLA_ARCHIVOS}}
            </tbody>
        </table>
        
        <div class="footer">
            <p>Pipeline de Procesamiento ENSU - Universidad Politécnica de Yucatán</p>
            <p>Generado automáticamente el {{FECHA_GENERACION}}</p>
        </div>
    </div>
</body>
</html>
"""
    
    # Preparar datos para el template
    fecha_generacion = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # Tabla de años
    tabla_años = ""
    for año_stat in estadisticas.get('estadisticas_por_año', []):
        tabla_años += f"""
            <tr>
                <td>{año_stat['año']}</td>
                <td>{año_stat['trimestres']}</td>
                <td>{año_stat['registros']}</td>
                <td class="success">{año_stat['promedio_pct_seguros']}%</td>
                <td class="warning">{año_stat['promedio_pct_inseguros']}%</td>
            </tr>
        """
    
    # Tabla de archivos procesados
    tabla_archivos = ""
    for resultado in resumen_procesamiento.get('resultados', []):
        if resultado.get('trimestre'):
            tabla_archivos += f"""
                <tr>
                    <td>{resultado['archivo']}</td>
                    <td>{resultado['trimestre']['año']}</td>
                    <td>T{resultado['trimestre']['trimestre']}</td>
                    <td>{resultado['registros_yucatan']}</td>
                    <td>{resultado['municipios']}</td>
                </tr>
            """
    
    # Reemplazar placeholders
    html_content = html_content.replace('{{FECHA_GENERACION}}', fecha_generacion)
    html_content = html_content.replace('{{ARCHIVOS_EXITOSOS}}', str(resumen_procesamiento.get('archivos_exitosos', 0)))
    html_content = html_content.replace('{{TOTAL_ARCHIVOS}}', str(resumen_procesamiento.get('total_archivos', 0)))
    html_content = html_content.replace('{{TOTAL_REGISTROS}}', str(estadisticas.get('total_registros', 0)))
    html_content = html_content.replace('{{AÑOS_CUBIERTOS}}', f"{min(estadisticas.get('años_cubiertos', [2016]))}-{max(estadisticas.get('años_cubiertos', [2025]))}")
    html_content = html_content.replace('{{TRIMESTRES_TOTALES}}', str(estadisticas.get('trimestres_totales', 0)))
    html_content = html_content.replace('{{PROMEDIO_PCT_SEGUROS}}', str(estadisticas.get('promedio_pct_seguros', 0)))
    html_content = html_content.replace('{{PROMEDIO_PCT_INSEGUROS}}', str(estadisticas.get('promedio_pct_inseguros', 0)))
    html_content = html_content.replace('{{MUNICIPIOS_UNICOS}}', str(estadisticas.get('municipios_unicos', 0)))
    html_content = html_content.replace('{{CIUDADES_UNICAS}}', str(estadisticas.get('ciudades_unicas', 0)))
    html_content = html_content.replace('{{TABLA_AÑOS}}', tabla_años)
    html_content = html_content.replace('{{TABLA_ARCHIVOS}}', tabla_archivos)
    
    # Rankings
    municipio_seguro = estadisticas.get('municipio_mas_seguro', {})
    html_content = html_content.replace('{{MUNICIPIO_MAS_SEGURO}}', municipio_seguro.get('nombre', 'N/A'))
    html_content = html_content.replace('{{PCT_MAS_SEGURO}}', str(municipio_seguro.get('pct_promedio_seguros', 0)))
    
    municipio_inseguro = estadisticas.get('municipio_mas_inseguro', {})
    html_content = html_content.replace('{{MUNICIPIO_MAS_INSEGURO}}', municipio_inseguro.get('nombre', 'N/A'))
    html_content = html_content.replace('{{PCT_MAS_INSEGURO}}', str(municipio_inseguro.get('pct_promedio_inseguros', 0)))
    
    return html_content

def main():
    """
    Función principal de la Fase 5
    """
    logger.info("=== FASE 5: GENERACIÓN DE REPORTE FINAL ===")
    
    # Cargar resumen de procesamiento
    resumen_path = 'temp/resumen_procesamiento.json'
    if not Path(resumen_path).exists():
        logger.error(f"No se encontró el resumen de procesamiento en {resumen_path}")
        logger.error("Ejecute primero la Fase 4")
        return False
    
    with open(resumen_path, 'r', encoding='utf-8') as f:
        resumen_procesamiento = json.load(f)
    
    # Cargar todos los datasets procesados
    df_completo = cargar_datasets_procesados()
    
    if df_completo.empty:
        logger.error("No hay datos para generar el reporte")
        return False
    
    # Generar estadísticas globales
    logger.info("Generando estadísticas globales...")
    estadisticas = generar_estadisticas_globales(df_completo)
    
    # Guardar dataset maestro
    logger.info("Guardando dataset maestro...")
    df_completo.to_csv('reports/dataset_maestro_yucatan.csv', index=False)
    logger.info(f"  - Dataset maestro guardado: reports/dataset_maestro_yucatan.csv")
    
    # Generar reporte HTML
    logger.info("Generando reporte HTML...")
    html_content = generar_reporte_html(estadisticas, resumen_procesamiento)
    
    # Crear directorio de reportes si no existe
    os.makedirs('reports', exist_ok=True)
    
    # Guardar reporte HTML
    html_path = 'reports/reporte_ensu_yucatan.html'
    with open(html_path, 'w', encoding='utf-8') as f:
        f.write(html_content)
    logger.info(f"  - Reporte HTML guardado: {html_path}")
    
    # Guardar estadísticas en JSON
    json_path = 'reports/estadisticas_finales.json'
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump({
            'fecha_generacion': datetime.now().isoformat(),
            'estadisticas_globales': estadisticas,
            'resumen_procesamiento': resumen_procesamiento
        }, f, indent=2, ensure_ascii=False)
    logger.info(f"  - Estadísticas JSON guardadas: {json_path}")
    
    # Resumen final
    logger.info("\n=== RESUMEN FINAL ===")
    logger.info(f"Total de registros procesados: {estadisticas['total_registros']}")
    logger.info(f"Años cubiertos: {min(estadisticas['años_cubiertos'])} - {max(estadisticas['años_cubiertos'])}")
    logger.info(f"Municipios analizados: {estadisticas['municipios_unicos']}")
    logger.info(f"Percepción promedio de seguridad: {estadisticas['promedio_pct_seguros']}%")
    logger.info(f"Percepción promedio de inseguridad: {estadisticas['promedio_pct_inseguros']}%")
    
    logger.info("\n📊 Reportes generados:")
    logger.info("  1. reports/dataset_maestro_yucatan.csv")
    logger.info("  2. reports/reporte_ensu_yucatan.html")
    logger.info("  3. reports/estadisticas_finales.json")
    
    return True

if __name__ == "__main__":
    exito = main()
    exit(0 if exito else 1)
```

---

## Ejecución Completa

### Preparación: Guardar los Scripts

Antes de ejecutar, debes guardar cada script en su archivo correspondiente:

1. **Crear directorio de scripts** (si no existe):
   ```bash
   mkdir -p scripts
   ```

2. **Guardar cada script**:
   - Copia el código de la Fase 1 y guárdalo en `scripts/fase1_mapeo_descubrimiento.py`
   - Copia el código de la Fase 2 y guárdalo en `scripts/fase2_validacion.py`
   - Copia el código de la Fase 3 y guárdalo en `scripts/fase3_verificacion_incremental.py`
   - Copia el código de la Fase 4 y guárdalo en `scripts/fase4_procesamiento.py`
   - Copia el código de la Fase 5 y guárdalo en `scripts/fase5_reporte.py`
   - Copia el código del script de limpieza y guárdalo en `scripts/clean_temp_files.py`

3. **Hacer los scripts ejecutables** (opcional, en sistemas Unix/Linux/Mac):
   ```bash
   chmod +x scripts/*.py
   ```

### ⚠️ IMPORTANTE: Uso de uv para ejecución
**TODOS los comandos Python deben ejecutarse con `uv run` para usar el entorno virtual correcto:**
- ✅ Correcto: `uv run python scripts/script.py`
- ❌ Incorrecto: `python scripts/script.py`

El proyecto debe llamarse `security-perception` en el archivo `pyproject.toml`.

### Verificación Pre-ejecución

Antes de ejecutar, verifica que todo esté listo:

```bash
# Verificar estructura de directorios
ls -la data/ logs/ temp/ scripts/ reports/

# Verificar que los scripts existen
ls -la scripts/*.py

# Verificar que uv está configurado
uv pip list | grep pandas

# Verificar que hay archivos ENSU en data/
find data -name "*cb*.csv" -type f | head -5
```

### Comandos de Ejecución Secuencial

```bash
# No es necesario activar el entorno virtual con uv
# uv run lo activa automáticamente

# Fase 1: Mapeo y Descubrimiento
uv run python scripts/fase1_mapeo_descubrimiento.py

# Verificar resultado
cat temp/mapeo_archivos.json | head -20

# Fase 2: Validación
uv run python scripts/fase2_validacion.py

# Verificar validaciones
cat temp/archivos_validados.json | grep "es_procesable"

# Fase 3: Verificación Incremental
uv run python scripts/fase3_verificacion_incremental.py

# Verificar pendientes
cat temp/archivos_pendientes.json | grep "total_pendientes"

# Fase 4: Procesamiento
uv run python scripts/fase4_procesamiento.py

# Verificar archivos generados
ls -la data/yucatan_processed/

# Fase 5: Reporte Final
uv run python scripts/fase5_reporte.py

# Abrir reporte
open reports/reporte_final_*.html  # En Mac
# o
xdg-open reports/reporte_final_*.html  # En Linux
```

### Script de Ejecución Completa

Crear archivo `run_all_phases.sh`:

```bash
#!/bin/bash
# Script para ejecutar todas las fases del procesamiento ENSU

set -e  # Detener si hay errores

echo "========================================="
echo "INICIANDO PROCESAMIENTO COMPLETO ENSU"
echo "Fecha: $(date)"
echo "========================================="

# No es necesario activar entorno virtual con uv
# uv run lo maneja automáticamente

# Fase 1
echo -e "\n>>> Ejecutando Fase 1: Mapeo y Descubrimiento"
uv run python scripts/fase1_mapeo_descubrimiento.py
if [ $? -eq 0 ]; then
    echo "✓ Fase 1 completada"
else
    echo "✗ Error en Fase 1"
    exit 1
fi

# Fase 2
echo -e "\n>>> Ejecutando Fase 2: Validación"
uv run python scripts/fase2_validacion.py
if [ $? -eq 0 ]; then
    echo "✓ Fase 2 completada"
else
    echo "✗ Error en Fase 2"
    exit 1
fi

# Fase 3
echo -e "\n>>> Ejecutando Fase 3: Verificación Incremental"
uv run python scripts/fase3_verificacion_incremental.py
if [ $? -eq 0 ]; then
    echo "✓ Fase 3 completada"
else
    echo "✗ Error en Fase 3"
    exit 1
fi

# Fase 4
echo -e "\n>>> Ejecutando Fase 4: Procesamiento"
uv run python scripts/fase4_procesamiento.py
if [ $? -eq 0 ]; then
    echo "✓ Fase 4 completada"
else
    echo "✗ Error en Fase 4"
    exit 1
fi

# Fase 5
echo -e "\n>>> Ejecutando Fase 5: Reporte Final"
uv run python scripts/fase5_reporte.py
if [ $? -eq 0 ]; then
    echo "✓ Fase 5 completada"
else
    echo "✗ Error en Fase 5"
    exit 1
fi

echo -e "\n========================================="
echo "PROCESAMIENTO COMPLETADO EXITOSAMENTE"
echo "========================================="
echo "Archivos procesados en: data/yucatan_processed/"
echo "Reporte disponible en: reports/"
echo "Logs disponibles en: logs/"
```

Hacer ejecutable:
```bash
chmod +x run_all_phases.sh
./run_all_phases.sh
```

---

## Troubleshooting

### Errores Comunes y Soluciones

#### 1. Error: "No se encontró el archivo mapeo_archivos.json"
**Causa**: No se ejecutó la Fase 1 o falló.
**Solución**:
```bash
# Verificar que existe la carpeta data
ls -la data/

# Re-ejecutar Fase 1
uv run python scripts/fase1_mapeo_descubrimiento.py

# Verificar salida
cat temp/mapeo_archivos.json
```

#### 2. Error: "KeyError: BP1_1"
**Causa**: El archivo CSV no tiene la columna BP1_1.
**Solución**:
```bash
# Verificar columnas del archivo
head -1 archivo.csv | tr ',' '\n' | grep -n BP1_1

# El archivo será marcado como no procesable en Fase 2
```

#### 3. Error: "No se encontraron archivos CB"
**Causa**: Los archivos ENSU no siguen el patrón esperado.
**Solución**:
```bash
# Buscar manualmente archivos CSV
find data/ -name "*.csv" -type f

# Modificar el patrón de búsqueda en fase1_mapeo_descubrimiento.py
# Línea: if file.endswith('.csv') and 'cb' in file.lower():
```

#### 4. Warning: "Duplicados detectados"
**Causa**: Múltiples archivos para el mismo período.
**Solución**:
```bash
# Revisar log de Fase 2
grep "DUPLICADOS" logs/fase2_validacion_*.log

# Eliminar manualmente el archivo duplicado o renombrar
# Los duplicados no se procesarán automáticamente
```

#### 5. Error: "No se encontraron registros de Yucatán"
**Causa**: El formato de NOM_ENT varía según el año del archivo.
**Formatos conocidos**:
- 2016-2017: `'Yucatán\r'` (con tilde y retorno de carro)
- 2018-2021: `'Yucatan\r'` o `'Yucatan'` (sin tilde)
- 2022+: `'YUCATAN'` (mayúsculas sin tilde)

**Solución**:
```bash
# Verificar formato exacto de NOM_ENT en un archivo
uv run python -c "import pandas as pd; df = pd.read_csv('ruta/archivo.csv'); yuc = df[df['NOM_ENT'].str.contains('Yucat', case=False, na=False)]['NOM_ENT'].unique(); print('Formatos encontrados:', yuc)"

# El script actualizado maneja todos los formatos usando:
# df['NOM_ENT'].str.strip().str.upper().str.contains('YUCAT')
```

#### 6. Error: "'utf-8' codec can't decode byte"
**Causa**: Archivos __MACOSX son metadatos de macOS.
**Solución**:
```bash
# Los archivos __MACOSX son ignorados automáticamente
# Si persiste el problema, eliminar la carpeta __MACOSX
rm -rf data/__MACOSX
```

#### 5. Error: "PermissionError"
**Causa**: Sin permisos de escritura.
**Solución**:
```bash
# Dar permisos a las carpetas
chmod -R 755 data/yucatan_processed
chmod -R 755 logs
chmod -R 755 temp
chmod -R 755 reports
```

### Verificación de Integridad

Script para verificar integridad: `verify_integrity.py`

```python
#!/usr/bin/env python3
"""
Verificar integridad del procesamiento
"""

import os
import json
import sys

def check_structure():
    """Verificar estructura de directorios"""
    required_dirs = ['data', 'data/yucatan_processed', 'logs', 'temp', 'reports', 'scripts']
    missing = []
    
    for dir in required_dirs:
        if not os.path.exists(dir):
            missing.append(dir)
    
    if missing:
        print(f"✗ Directorios faltantes: {missing}")
        return False
    
    print("✓ Estructura de directorios correcta")
    return True

def check_intermediate_files():
    """Verificar archivos intermedios"""
    files = {
        'temp/mapeo_archivos.json': 'Fase 1',
        'temp/archivos_validados.json': 'Fase 2',
        'temp/archivos_pendientes.json': 'Fase 3',
        'temp/procesamiento_resultados.json': 'Fase 4'
    }
    
    for file, fase in files.items():
        if os.path.exists(file):
            with open(file, 'r') as f:
                data = json.load(f)
            print(f"✓ {fase}: {file} existe")
        else:
            print(f"✗ {fase}: {file} no encontrado")

def check_processed_files():
    """Verificar archivos procesados"""
    processed_dir = 'data/yucatan_processed'
    if os.path.exists(processed_dir):
        files = [f for f in os.listdir(processed_dir) if f.endswith('.csv')]
        print(f"✓ Archivos procesados: {len(files)}")
        for f in files:
            print(f"  - {f}")
    else:
        print("✗ No existe directorio de archivos procesados")

def main():
    print("=== VERIFICACIÓN DE INTEGRIDAD ===\n")
    
    if not check_structure():
        sys.exit(1)
    
    check_intermediate_files()
    check_processed_files()
    
    print("\n=== FIN DE VERIFICACIÓN ===")

if __name__ == "__main__":
    main()
```

### Recuperación de Errores

Si una fase falla, puedes:

1. **Revisar el log específico**:
```bash
tail -100 logs/fase3_verificacion_*.log
```

2. **Ejecutar en modo debug**:
Agregar al inicio del script problemático:
```python
import pdb
pdb.set_trace()  # Punto de interrupción
```

3. **Re-ejecutar desde la fase fallida**:
No es necesario volver a ejecutar todas las fases, solo desde donde falló.

4. **Limpiar y reiniciar** (último recurso):
```bash
# Backup de procesados
cp -r data/yucatan_processed data/yucatan_processed.backup

# Limpiar temporales
rm temp/*.json

# Re-ejecutar todo
./run_all_phases.sh
```

---

## Anexos

### A. Manejo de Períodos No Identificados (UNKNOWN)

Los archivos que no pueden ser identificados automáticamente se marcan con período `UNKNOWN_XXX`. Estos requieren intervención manual:

1. **Identificación Manual**:
   ```bash
   # Ver archivos con período UNKNOWN
   cat temp/mapeo_archivos.json | grep -A 5 "UNKNOWN"
   ```

2. **Verificación del Contenido**:
   ```bash
   # Inspeccionar manualmente el archivo CSV (usando 5 filas estándar)
   uv run python -c "
   import pandas as pd
   df = pd.read_csv('ruta/al/archivo.csv', nrows=5)
   print(df[['PER', 'PERIODO', 'periodo']].head() if any(col in df.columns for col in ['PER', 'PERIODO', 'periodo']) else 'No se encontró columna de período')
   "
   ```

3. **Renombrar Archivos**:
   Si identificas el período correcto, renombra el archivo para incluir el formato esperado (YYYY_Qt):
   ```bash
   mv data/archivo_original.csv data/archivo_ensu_cb_2025_2t.csv
   ```

4. **Re-ejecutar Fase 1**:
   Después de renombrar, ejecuta nuevamente la Fase 1 para actualizar el mapeo.

### B. Glosario de Términos ENSU

- **ENSU**: Encuesta Nacional de Seguridad Pública Urbana
- **BP1_1**: Pregunta sobre percepción de seguridad (1=Seguro, 2=Inseguro, 9=No sabe/No responde)
- **CB**: Cuestionario Básico
- **CS**: Cuestionario Sociodemográfico
- **VIV**: Vivienda
- **NOM_ENT**: Nombre de la Entidad Federativa
- **NOM_MUN**: Nombre del Municipio
- **NOM_CD**: Nombre de la Ciudad
- **PER**: Período de la encuesta (formato MMYY)

### C. Estructura de Archivos ENSU

```
conjunto_de_datos_ensu_YYYY_Nt_csv/
├── conjunto_de_datos_ensu_cb_MMYY/
│   ├── catalogos/              # Catálogos de códigos
│   ├── conjunto_de_datos/      # Datos principales
│   │   └── conjunto_de_datos_ensu_cb_MMYY.csv
│   ├── diccionario_de_datos/   # Diccionario
│   └── metadatos/              # Metadatos
├── conjunto_de_datos_ensu_cs_MMYY/
└── conjunto_de_datos_ensu_viv_MMYY/
```

### D. Interpretación de Resultados

#### Niveles de Percepción de Seguridad:
- **Muy Seguro**: > 70% respuestas "Seguro"
- **Seguro**: 50-70% respuestas "Seguro"
- **Inseguro**: 30-50% respuestas "Seguro"
- **Muy Inseguro**: < 30% respuestas "Seguro"

#### Calidad de Datos:
- **Excelente**: < 5% "No sabe/No responde"
- **Buena**: 5-10% "No sabe/No responde"
- **Regular**: 10-20% "No sabe/No responde"
- **Pobre**: > 20% "No sabe/No responde"

### E. Notas sobre Lectura de Archivos CSV

**Estandarización**: Todos los scripts usan consistentemente `nrows=5` cuando necesitan leer una muestra del archivo para:
- Detección de período
- Validación de columnas
- Verificación manual

Esto asegura consistencia y reduce el uso de memoria al procesar archivos grandes.

### F. Script de Limpieza de Archivos Temporales

Para mantener el sistema limpio y evitar acumulación de archivos temporales antiguos:

#### Script: `clean_temp_files.py`

**IMPORTANTE**: Guarda el siguiente código en el archivo `scripts/clean_temp_files.py`

```python
#!/usr/bin/env python3
"""
Script de limpieza de archivos temporales y logs antiguos
"""

import os
import json
import shutil
from datetime import datetime, timedelta
from pathlib import Path

def setup_logging():
    """Configurar logging para limpieza"""
    import logging
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = f"logs/limpieza_{timestamp}.log"
    
    os.makedirs("logs", exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] %(levelname)s: %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def clean_old_logs(days_to_keep=30):
    """Limpiar logs más antiguos que N días"""
    logger = logging.getLogger(__name__)
    log_dir = Path("logs")
    
    if not log_dir.exists():
        logger.info("No existe directorio de logs")
        return
    
    cutoff_date = datetime.now() - timedelta(days=days_to_keep)
    deleted_count = 0
    
    for log_file in log_dir.glob("*.log"):
        try:
            file_time = datetime.fromtimestamp(log_file.stat().st_mtime)
            if file_time < cutoff_date:
                logger.info(f"Eliminando log antiguo: {log_file.name} ({file_time.date()})")
                log_file.unlink()
                deleted_count += 1
        except Exception as e:
            logger.error(f"Error eliminando {log_file}: {e}")
    
    logger.info(f"Logs eliminados: {deleted_count}")
    return deleted_count

def clean_temp_json(keep_latest=True):
    """Limpiar archivos JSON temporales, opcionalmente manteniendo los más recientes"""
    logger = logging.getLogger(__name__)
    temp_dir = Path("temp")
    
    if not temp_dir.exists():
        logger.info("No existe directorio temp")
        return
    
    json_files = {
        'mapeo_archivos.json': [],
        'archivos_validados.json': [],
        'archivos_pendientes.json': [],
        'procesamiento_resultados.json': []
    }
    
    # Buscar todos los archivos JSON con timestamp
    for json_file in temp_dir.glob("*.json"):
        base_name = json_file.name
        
        # Si queremos mantener el más reciente
        if keep_latest and base_name in json_files:
            continue  # No eliminar archivos base
        
        # Eliminar archivos con timestamp o versiones antiguas
        if '_backup' in base_name or '_old' in base_name:
            try:
                logger.info(f"Eliminando archivo temporal: {json_file.name}")
                json_file.unlink()
            except Exception as e:
                logger.error(f"Error eliminando {json_file}: {e}")
    
    logger.info("Limpieza de archivos temporales completada")

def create_backup(source_dir="data/yucatan_processed", backup_dir="backups"):
    """Crear backup de archivos procesados"""
    logger = logging.getLogger(__name__)
    
    if not Path(source_dir).exists():
        logger.warning(f"No existe directorio fuente: {source_dir}")
        return
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = Path(backup_dir) / f"backup_{timestamp}"
    
    try:
        shutil.copytree(source_dir, backup_path)
        logger.info(f"Backup creado en: {backup_path}")
        
        # Limpiar backups antiguos (mantener solo los últimos 5)
        all_backups = sorted(Path(backup_dir).glob("backup_*"))
        if len(all_backups) > 5:
            for old_backup in all_backups[:-5]:
                logger.info(f"Eliminando backup antiguo: {old_backup}")
                shutil.rmtree(old_backup)
                
    except Exception as e:
        logger.error(f"Error creando backup: {e}")

def get_directory_size(path):
    """Obtener tamaño total de un directorio"""
    total = 0
    for entry in Path(path).rglob('*'):
        if entry.is_file():
            total += entry.stat().st_size
    return total / (1024 * 1024)  # Retornar en MB

def main():
    logger = setup_logging()
    
    logger.info("=== INICIANDO LIMPIEZA DE ARCHIVOS TEMPORALES ===")
    
    # Mostrar estado actual
    logger.info("Estado actual del sistema:")
    directories = ['logs', 'temp', 'data/yucatan_processed']
    for dir_path in directories:
        if Path(dir_path).exists():
            size_mb = get_directory_size(dir_path)
            file_count = len(list(Path(dir_path).rglob('*')))
            logger.info(f"  {dir_path}: {file_count} archivos, {size_mb:.2f} MB")
    
    # Realizar limpieza
    logger.info("\nRealizando limpieza...")
    
    # 1. Limpiar logs antiguos (más de 30 días)
    clean_old_logs(days_to_keep=30)
    
    # 2. Limpiar archivos temporales
    clean_temp_json(keep_latest=True)
    
    # 3. Crear backup antes de limpiar procesados
    create_backup()
    
    # Mostrar estado final
    logger.info("\nEstado después de limpieza:")
    for dir_path in directories:
        if Path(dir_path).exists():
            size_mb = get_directory_size(dir_path)
            file_count = len(list(Path(dir_path).rglob('*')))
            logger.info(f"  {dir_path}: {file_count} archivos, {size_mb:.2f} MB")
    
    logger.info("=== LIMPIEZA COMPLETADA ===")

if __name__ == "__main__":
    main()
```

#### Uso del Script de Limpieza:

```bash
# Ejecutar limpieza manual
uv run python scripts/clean_temp_files.py

# Programar limpieza automática (crontab)
# Agregar a crontab -e:
0 2 * * 0 cd /ruta/al/proyecto && uv run python scripts/clean_temp_files.py
```

### G. Extensiones Futuras

1. **Análisis Comparativo entre Estados**
2. **Predicción de Tendencias** usando ML
3. **Visualización Geoespacial** con mapas
4. **API REST** para consultar datos procesados
5. **Dashboard Interactivo** con Dash/Streamlit

---

## Conclusión

Este documento proporciona una guía completa y replicable para procesar datos ENSU de Yucatán. Siguiendo las 5 fases descritas, cualquier usuario puede:

1. Identificar y validar archivos ENSU disponibles
2. Procesar incrementalmente solo archivos nuevos
3. Generar estadísticas de percepción de seguridad
4. Crear reportes visuales y análisis de tendencias

El sistema incluye:
- **Logging verboso** para debugging y auditoría
- **Manejo inteligente de duplicados** seleccionando el mejor archivo
- **Validaciones exhaustivas** de datos con advertencias apropiadas
- **Procesamiento incremental** para eficiencia
- **Limpieza automática** de archivos temporales

Para soporte adicional, revisar los logs generados en cada fase y seguir las guías de troubleshooting incluidas.