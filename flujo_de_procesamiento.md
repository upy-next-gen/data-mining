# Flujo de Procesamiento de Datos ENSU para Percepción de Seguridad en Yucatán

## Objetivo

Este documento detalla el procedimiento para procesar los microdatos de la Encuesta Nacional de Seguridad Pública Urbana (ENSU), con el fin de generar un dataset consolidado sobre la percepción de seguridad, filtrado exclusivamente para la entidad de Yucatán. Las instrucciones aquí presentes deben ser utilizadas para generar los scripts de procesamiento correspondientes.

---

## Paso 1: Preparación del Entorno

1.  **Crear Directorio de Salida:**
    *   Asegúrate de que exista el directorio donde se guardarán los resultados. Si no existe, créalo.
    *   **Ruta de destino:** `data/yucatan-inseguridad/`

2.  **Configurar Archivo de Log:**
    *   Se debe generar un archivo de registro para trazar la ejecución del proceso.
    *   **Nombre del archivo:** `procesamiento.log`

3.  **Verificar Dependencias:**
    *   El procesamiento de datos requiere la librería `pandas`. Verifica que esté listada en el `pyproject.toml`. Si no, debe ser añadida como dependencia del proyecto.

---

## Paso 2: Identificación de los Datasets de Origen

1.  **Criterio de Búsqueda:**
    *   Se deben localizar todos los archivos con extensión `.csv` que se encuentren dentro del directorio `Data/`.
    *   La condición principal es que la ruta de cada archivo `.csv` debe contener un directorio cuyo nombre incluya la cadena `cb` (ej: `.../conjunto_de_datos_cb_ensu_01_2015/...`).
    *   Se debe generar una lista con las rutas absolutas de todos los archivos que cumplan esta condición.

---

## Paso 2.5: Inspección de Archivos y Definición del Mapeo de Columnas (Paso Crítico)

1.  **Necesidad:** Antes de escribir el script de procesamiento, es indispensable conocer las variaciones en los nombres de las columnas a lo largo del tiempo. El script depende de un mapa de normalización completo para funcionar.

2.  **Acción Requerida:**
    *   Selecciona una **muestra representativa** de los archivos CSV identificados en el Paso 2. Se recomienda elegir al menos tres archivos: uno de un año antiguo (ej. 2015), uno intermedio (ej. 2018) y uno reciente (ej. 2022).
    *   Lee las primeras líneas (la cabecera) de cada archivo muestra para identificar los nombres exactos utilizados para la **entidad**, el **municipio** y la **percepción de seguridad (`BP1_1`)**.
    *   Utiliza la información recolectada para **completar de forma exhaustiva** el diccionario `MAPEO_COLUMNAS` que se usará en el Paso 3. Este mapa es la clave para la estandarización.

3.  **Ejemplo de Mapa Completado (Ilustrativo):**
    *   El mapa final debería lucir similar a este, conteniendo todas las variaciones encontradas:
        ```python
        # Ejemplo de mapa completado tras la inspección:
        MAPEO_COLUMNAS = {
            # Variaciones para NOM_ENT
            "NOM_ENT": "NOM_ENT",
            "ENTIDAD": "NOM_ENT",
            # Variaciones para NOM_MUN
            "NOM_MUN": "NOM_MUN",
            "MUN": "NOM_MUN",
            # Variaciones para BP1_1
            "BP1_1": "BP1_1",
            "P1_1": "BP1_1", # Ejemplo de variación encontrada
        }
        ```

---

## Paso 3: Creación del Script de Lógica de Procesamiento (`procesar_datos.py`)

Este script no se ejecutará directamente, sino que contendrá una función principal que encapsule la lógica de negocio para procesar un único archivo.

1.  **Función Principal:**
    *   Define una función `procesar_archivo(ruta_origen: str, dir_destino: str, logger: object)`.

2.  **Mapeo de Normalización de Columnas:**
    *   Dentro del script, define el diccionario `MAPEO_COLUMNAS` **ya completado** según la investigación del Paso 2.5.

3.  **Lógica dentro de `procesar_archivo`:**
    *   **Lectura y Normalización:** Lee el archivo CSV ubicado en `ruta_origen` usando `pandas`. Renombra las columnas del DataFrame utilizando el `MAPEO_COLUMNAS`.
    *   **Validación de Archivo:**
        *   Comprueba si las columnas `['NOM_ENT', 'NOM_MUN', 'BP1_1']` existen en el DataFrame después de la normalización.
        *   Si alguna de estas columnas falta, la función debe registrar un error crítico en el `logger` (incluyendo el nombre del archivo) y terminar su ejecución para ese archivo.
    *   **Limpieza y Validación de Datos:**
        *   Convierte la columna `BP1_1` a un tipo numérico, forzando los errores de conversión a `NaN`.
        *   Identifica todas las filas donde `BP1_1` **no** sea `1`, `2` o `9`.
        *   Para cada fila inválida, registra una advertencia en el `logger`.
        *   Filtra el DataFrame para **excluir** estas filas inválidas. El `TOTAL_REGISTROS` se calculará sobre los datos válidos restantes.
    *   **Extracción de Metadatos:**
        *   Analiza la cadena `ruta_origen` para extraer el `AÑO` y el `TRIMESTRE` del dataset. Se recomienda usar expresiones regulares para una extracción robusta.
    *   **Agregación de Datos:**
        *   Agrupa el DataFrame por `['NOM_ENT', 'NOM_MUN']`.
        *   Para cada grupo, calcula las siguientes agregaciones:
            *   `TOTAL_REGISTROS`: Conteo total de registros en el grupo.
            *   `TOTAL_SEGUROS`: Conteo de filas donde `BP1_1 == 1`.
            *   `TOTAL_INSEGUROS`: Conteo de filas donde `BP1_1 == 2`.
            *   `TOTAL_NO_RESPONDE`: Conteo de filas donde `BP1_1 == 9`.
    *   **Cálculo de Porcentajes:**
        *   Sobre el DataFrame agregado, calcula las siguientes columnas:
            *   `PCT_SEGUROS` = (`TOTAL_SEGUROS` / `TOTAL_REGISTROS`) * 100
            *   `PCT_INSEGUROS` = (`TOTAL_INSEGUROS` / `TOTAL_REGISTROS`) * 100
            *   `PCT_NO_RESPONDE` = (`TOTAL_NO_RESPONDE` / `TOTAL_REGISTROS`) * 100
        *   Asegúrate de manejar posibles divisiones por cero.
    *   **Filtrado por Entidad (Requisito Clave):**
        *   Filtra el DataFrame agregado para conservar únicamente las filas donde el valor de la columna `NOM_ENT`, después de limpiar espacios y convertir a mayúsculas, sea igual a `'YUCATAN'`.
    *   **Generación de Salida:**
        *   Si el DataFrame resultante después del filtrado no está vacío, guárdalo.
        *   Crea un nombre de archivo descriptivo, ej: `resultado_yucatan_{AÑO}_T{TRIMESTRE}.csv`.
        *   Guarda el DataFrame final en un archivo CSV en el directorio `dir_destino`.

---

## Paso 4: Creación del Script Orquestador (`main.py`)

Este script será el punto de entrada para ejecutar todo el proceso.

1.  **Configuración del Logging:**
    *   Configura el módulo `logging` de Python para que escriba en el archivo `procesamiento.log`.
    *   El formato del log debe ser verboso, incluyendo fecha, hora, nivel del log y el mensaje.

2.  **Lógica de Orquestación:**
    *   Importa la función `procesar_archivo` desde `procesar_datos.py`.
    *   Obtén la lista de todos los archivos a procesar (ver Paso 2).
    *   Crea el directorio de destino (ver Paso 1).
    *   Itera a través de la lista de rutas de archivos. En cada iteración, llama a `procesar_archivo()` con los parámetros correspondientes (ruta del archivo, directorio de destino, y el objeto logger).

---

## Paso 5: Ejecución

*   Para iniciar el proceso completo, ejecuta el script `main.py` desde la terminal.
*   Una vez finalizado, verifica la carpeta `data/yucatan-inseguridad/` para los CSV generados y revisa el archivo `procesamiento.log` para un resumen detallado de la ejecución.