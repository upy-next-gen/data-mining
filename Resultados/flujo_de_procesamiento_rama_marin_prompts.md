# Flujo de Procesamiento Simplificado: Análisis de Seguridad en Yucatán

## 1. Introducción

Este documento describe el flujo de trabajo consolidado y simplificado para procesar los microdatos de la Encuesta Nacional de Seguridad Pública Urbana (ENSU), con el objetivo de analizar la percepción de seguridad exclusivamente para el estado de Yucatán.

A diferencia de un enfoque multifase, este proceso utiliza un **único script de Python (`process_yucatan_data.py`)** que orquesta todo el pipeline, desde la lectura de los datos brutos hasta la generación de los archivos CSV procesados.

### Objetivo

El objetivo principal es identificar los datasets de la ENSU, filtrar los registros pertenecientes a Yucatán, limpiar y normalizar los datos, y generar un reporte agregado por municipio con las estadísticas de percepción de seguridad.

### Flujo de Datos

El flujo de datos es lineal y directo:

```
Directorio de datos brutos (`data/`)
 |
 +-- conjunto_de_datos_ensu_2023_1t_csv/
 |   +-- .../*cb*.csv
 +-- conjunto_de_datos_ensu_2023_2t_csv/
 |   +-- .../*cb*.csv
 |
 V
(Ejecución del script `process_yucatan_data.py`)
 |
 V
Directorio de datos procesados (`data/yucatan-inseguridad/`)
 |
 +-- procesado_2023_1t_cb.csv
 +-- procesado_2023_2t_cb.csv
 |
 V
Archivo de Log (`logs/yucatan_processing.log`)
```

---

## 2. Lógica del Proceso

El corazón del pipeline es el script `process_yucatan_data.py`. A continuación, se detalla la lógica encapsulada en él.

### Paso 1: Descubrimiento y Lectura
- **Búsqueda de Archivos**: El script escanea recursivamente el directorio `data/` en busca de cualquier archivo CSV cuyo nombre contenga `cb`.
- **Extracción de Metadatos**: De la ruta de cada archivo encontrado, se extraen el **Año** y el **Trimestre** correspondientes.
- **Lectura Segura**: Se intenta leer cada archivo con codificación `UTF-8`. Si falla, se reintenta con `latin1` para maximizar la compatibilidad con archivos de diferentes épocas.

### Paso 2: Validación y Normalización
- **Normalización de Columnas**: Los nombres de todas las columnas se normalizan a un formato estándar (minúsculas, sin acentos, espacios a guiones bajos) para asegurar que se puedan encontrar `nom_ent`, `nom_mun` y `bp1_1` sin importar las variaciones.
- **Validación de Columnas**: Se verifica que las columnas esenciales (`NOM_ENT`, `NOM_MUN`, `BP1_1`) existan en el archivo. Si no, el archivo se omite y el evento se registra en el log.

### Paso 3: Limpieza y Filtrado
Este es el paso más crítico del proceso.

1.  **Normalización de Texto**: Los datos en las columnas `NOM_ENT` y `NOM_MUN` se estandarizan:
    - Se convierten a mayúsculas.
    - Se eliminan acentos.
    - La letra `Ñ` se reemplaza por `N`.
    Esto es fundamental para poder agrupar y filtrar de manera consistente.

2.  **Filtrado por Entidad**: El script **descarta todos los registros que no pertenecen a "YUCATAN"**.

3.  **Limpieza de Nulos**: Se eliminan las filas donde `NOM_ENT` o `NOM_MUN` no tengan valor.

4.  **Limpieza de Datos Inválidos**: Se eliminan las filas donde la columna `BP1_1` (percepción de seguridad) contenga valores diferentes a `1` (Seguro), `2` (Inseguro) o `9` (No responde).

Todos los descartes y limpiezas se registran en el archivo de log para trazabilidad.

### Paso 4: Agregación y Cálculo
- **Agrupación**: Los datos limpios y filtrados para Yucatán se agrupan por `NOM_MUN`.
- **Cálculo de Estadísticas**: Para cada municipio, se calculan las siguientes métricas:
  - `TOTAL_REGISTROS`: Conteo total de respuestas.
  - `TOTAL_SEGUROS`: Conteo de respuestas "1".
  - `TOTAL_INSEGUROS`: Conteo de respuestas "2".
  - `TOTAL_NO_RESPONDE`: Conteo de respuestas "9".
  - `PCT_SEGUROS`: Porcentaje de respuestas "1".
  - `PCT_INSEGUROS`: Porcentaje de respuestas "2".
  - `PCT_NO_RESPONDE`: Porcentaje de respuestas "9".

### Paso 5: Generación de Archivos de Salida
- **Archivos CSV Procesados**: Por cada archivo de entrada que contenía datos de Yucatán, se genera un nuevo archivo CSV en `data/yucatan-inseguridad/` con el nombre `procesado_YYYY_QT_cb.csv`.
- **Log de Ejecución**: Se genera un único archivo `logs/yucatan_processing.log` que documenta de forma verbosa toda la ejecución: archivos leídos, omitidos, errores, número de filas filtradas, etc.

---

## 3. Requisitos y Ejecución

### Requisitos del Entorno
- **Python 3**: Una instalación funcional de Python 3.
- **Librería `pandas`**: Necesaria para la manipulación de datos. Se instala con el gestor de paquetes `pip`.
  ```bash
  # Instalar pip (si no está disponible)
  sudo apt-get update && sudo apt-get install -y python3-pip

  # Instalar pandas
  python3 -m pip install pandas
  ```

### Ejecución Manual
Para ejecutar el proceso, simplemente corra el script desde la raíz del proyecto.

```bash
python3 process_yucatan_data.py
```
O, si se ejecuta desde Windows PowerShell, usando la ruta completa al intérprete y al script:
```powershell
& C:/Users/ramam/AppData/Local/Microsoft/WindowsApps/python3.11.exe "c:/Users/ramam/OneDrive/Escritorio/Data Mining/data-mining/process_yucatan_data.py"
```

El script se encargará de encontrar los archivos, procesarlos y guardar los resultados en las carpetas correspondientes.

---

## 4. Estructura del Archivo de Salida

Cada archivo `procesado_YYYY_QT_cb.csv` generado contendrá las siguientes columnas:

| Columna             | Descripción                                         |
|---------------------|-----------------------------------------------------|
| `NOM_ENT`           | Nombre de la entidad (siempre "YUCATAN")            |
| `NOM_MUN`           | Nombre del municipio (normalizado)                  |
| `TOTAL_REGISTROS`   | Total de encuestas válidas para ese municipio       |
| `TOTAL_SEGUROS`     | Total de personas que respondieron "Seguro"         |
| `TOTAL_INSEGUROS`   | Total de personas que respondieron "Inseguro"       |
| `TOTAL_NO_RESPONDE` | Total de personas que no respondieron la pregunta   |
| `PCT_SEGUROS`       | Porcentaje de percepción de seguridad               |
| `PCT_INSEGUROS`     | Porcentaje de percepción de inseguridad             |
| `PCT_NO_RESPONDE`   | Porcentaje de no respuesta                          |
| `AÑO`               | Año de la encuesta                                  |
| `TRIMESTRE`         | Trimestre de la encuesta                            |

