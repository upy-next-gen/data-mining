# Sistema de Análisis de Percepción de Seguridad en Yucatán

Este sistema procesa datos de la Encuesta Nacional de Seguridad Pública Urbana (ENSU) del INEGI para generar reportes visuales y análisis de la percepción de seguridad en el estado de Yucatán.

## Características

- Procesamiento automatizado de datos ENSU
- Normalización de nombres y validación de datos
- Agregación por entidad-municipio con cálculo de porcentajes
- Detección de año/trimestre desde nombres de archivos
- Generación de reportes HTML interactivos con:
  - Gráficos de evolución temporal a nivel estatal
  - Gráficos individuales por municipio
  - Tabla pivote completa con porcentajes
  - Notas metodológicas
- Logging detallado en archivos separados
- Manejo de casos con datos faltantes (gaps temporales)
- Soporte para procesamiento en lotes (chunks) para archivos grandes

## Prerrequisitos

- Python 3.10 o superior
- Dependencias especificadas en `requirements.txt`

## Instalación

1. Clone este repositorio o descomprima el archivo del proyecto

2. Instale las dependencias:

```bash
pip install -r requirements.txt
```

## Estructura de directorios

```
/
├── data/
│   └── yucatan-inseguridad/
│       ├── reportes/
│       │   ├── reporte_yucatan_evolucion.html
│       │   ├── log_analisis_yucatan.log
│       │   └── datos_tabulados_yucatan.csv
│       └── logs/
│           └── procesamiento_log_YYYYMMDD_HHMMSS.log
├── conjunto_de_datos_ensu_cb_0625.csv    # Archivo de entrada
├── diccionario_de_datos_ensu_cb_0625.csv # Diccionario de datos (opcional)
├── procesar_ensu_cb.py                   # Script de procesamiento 
├── generar_reporte_evolucion.py          # Script de generación de reportes
└── requirements.txt                      # Dependencias
```

## Ejecución

### 1. Procesamiento de datos

Este script procesa los datos de la ENSU y genera un archivo CSV agregado por municipio.

```bash
python procesar_ensu_cb.py --estado YUCATAN
```

Opciones adicionales:
- `--salida`: Directorio de salida principal (default: `data/yucatan-inseguridad/`)
- `--reportes`: Directorio de reportes (default: `data/yucatan-inseguridad/reportes/`)
- `--logs`: Directorio de logs (default: `data/yucatan-inseguridad/logs/`)
- `--chunksize`: Tamaño del chunk para procesamiento por lotes
- `--timestamped`: Agrega timestamp a los nombres de archivo de salida

### 2. Generación de reportes

Este script genera un reporte HTML con visualizaciones y tabla pivote.

```bash
python generar_reporte_evolucion.py
```

Opciones adicionales:
- `--estado`: Estado a filtrar (default: YUCATAN)
- `--salida`: Directorio de salida principal (default: `data/yucatan-inseguridad/`)
- `--reportes`: Directorio de reportes (default: `data/yucatan-inseguridad/reportes/`)
- `--logs`: Directorio de logs (default: `data/yucatan-inseguridad/logs/`)
- `--timestamped`: Agrega timestamp a los nombres de archivo de salida

## Notas de metodología

- El procesamiento calcula porcentajes de percepción de seguridad basados en la variable `BP1_1` del dataset ENSU, donde:
  - 1 = Seguro
  - 2 = Inseguro
  - 9 = No responde
- Los porcentajes se calculan como (total_categoría / total_registros) * 100
- El promedio estatal es un promedio ponderado por el número de respuestas de cada municipio
- Los nombres de entidades y municipios son normalizados (mayúsculas, sin acentos)
- Se detectan y manejan periodos faltantes en las series temporales

## Extensibilidad

El sistema está diseñado para soportar:
- Procesamiento de múltiples archivos ENSU
- Detección y corrección de duplicados de periodos
- Extensión a otras entidades federativas
- Personalización del formato de los reportes
