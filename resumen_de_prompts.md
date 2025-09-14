# Resumen de Instrucciones de la Sesión

Este documento resume los principales prompts e instrucciones proporcionados durante la sesión para el análisis de datos de percepción de seguridad.

## 1. Procesamiento Inicial de CSV

- **Instrucción:** Crear un script de Python para procesar un archivo CSV específico (`diccionario_de_datos_ensu_cb_0625.csv`) usando `pandas`.
- **Objetivo:** Extraer las columnas `NOM_ENT`, `NOM_MUN`, y `BP_1` en un nuevo dataset.

## 2. Mapeo y Procesamiento por Lotes

- **Instrucción:** Mapear todos los datasets de la carpeta `data` que contuvieran "cb" en su nombre y tuvieran las columnas `NOM_ENT`, `NOM_MUN`, y `BP1_1`.
- **Objetivo:** Procesar cada uno de estos datasets para generar un nuevo archivo de resultados con las siguientes columnas: `NOM_ENT`, `NOM_MUN`, `NOM_CD`, `TOTAL_REGISTROS`, `TOTAL_SEGUROS`, `TOTAL_INSEGUROS`, `TOTAL_NO_RESPONDE`, `PCT_SEGUROS`, `PCT_INSEGUROS`, `PCT_NO_RESPONDE`, `AÑO`, `TRIMESTRE`.
- **Detalles de Salida:** Crear una carpeta `datasets resultantes` y generar un archivo de salida por cada archivo de entrada, además de un archivo consolidado.

## 3. Refinamiento y Normalización de Archivos

- **Instrucción:** Ante la inconsistencia en los nombres de los archivos, renombrarlos a un formato estándar para facilitar el mapeo de año y trimestre.
- **Decisión Posterior:** Se optó por no borrar los datos originales y mantener una carpeta (`data_limpia`) como evidencia del proceso de limpieza.

## 4. Especificaciones Detalladas del Procesamiento

- **Instrucción:** Se proporcionaron múltiples requisitos técnicos y de negocio:
    - **Excluir `NOM_CD`** del análisis.
    - **Filtrar** los datos para procesar únicamente aquellos donde `NOM_ENT` fuera 'YUCATAN' o 'yucatan'.
    - **Normalizar** los datos de texto (convertir a mayúsculas, `Ñ` a `N`, y quitar acentos).
    - **Validar** que los datasets tuvieran las columnas requeridas y los rangos de datos correctos.
    - **Manejo de Errores**: Excluir y registrar en un log los registros con valores inválidos o faltantes en columnas clave.
    - **Salida**: Guardar los archivos procesados en la carpeta `data/yucatan-inseguridad/` con el formato `procesado_YYYY_QT_cb.csv`.
    - **Logging**: Generar un archivo de log verboso para documentar todo el proceso.

## 5. Cambio de Formato del Reporte

- **Instrucción Inicial:** Crear un reporte en formato HTML interactivo.
- **Instrucción Posterior:** Cambiar el formato del reporte a Markdown (`.md`) para que fuera similar al de otros compañeros.
- **Requisito del Reporte MD:**
    - Incluir un gráfico estático (`.png`) de la tendencia de inseguridad, generado con `matplotlib`.
    - Contener encabezado, resumen por trimestre, la tabla de datos y una sección de conclusiones.

## 6. Limpieza Final del Espacio de Trabajo

- **Instrucción:**
    - Eliminar la carpeta de resultados (`datasets resultantes`).
    - Eliminar los scripts de Python intermedios.
    - Crear una carpeta `python scripts` y mover allí los scripts finales y principales (`main.py` y `process_yucatan_data.py`).
