# Reporte de Verificación de Datos

## Objetivo
El objetivo de este proceso fue verificar que los valores en la columna `BP1_1` del archivo `datos_procesados.csv` se encontraran exclusivamente dentro del rango `[1, 2, 9]`.

## Proceso de Verificación

Se siguieron los siguientes pasos para completar la verificación:

### 1. Análisis Inicial
Para entender el origen de los datos, primero se inspeccionó el archivo `process_data.py`. Se determinó que este script procesa un archivo CSV fuente y genera el archivo `datos_procesados.csv`, extrayendo un subconjunto de columnas, incluyendo la columna de interés `BP1_1`.

### 2. Creación de Script de Verificación
Se creó un script de Python (`check_values.py`) para realizar la validación. El script utiliza la librería `pandas` para leer el archivo `datos_procesados.csv` y obtener los valores únicos de la columna `BP1_1`.

El contenido del script fue el siguiente:
```python
import pandas as pd

def check_bp1_1_values(file_path):
    """
    Checks the unique values of the 'BP1_1' column in a CSV file.

    Args:
        file_path (str): The path to the CSV file.
    """
    try:
        df = pd.read_csv(file_path)
        unique_values = df['BP1_1'].unique()
        
        print(f"Valores unicos en la columna BP1_1: {unique_values}")
        
        allowed_values = {1, 2, 9}
        
        # Convert unique_values to a set for comparison
        unique_values_set = set(unique_values)
        
        if unique_values_set.issubset(allowed_values):
            print("Todos los valores en la columna BP1_1 estan dentro del rango permitido (1, 2, 9).")
        else:
            print("Algunos valores en la columna BP1_1 estan fuera del rango permitido (1, 2, 9).")
            
    except FileNotFoundError:
        print(f"Error: El archivo no fue encontrado en la ruta: {file_path}")
    except KeyError:
        print("Error: La columna 'BP1_1' no fue encontrada en el archivo.")
    except Exception as e:
        print(f"Un error inesperado ocurrio: {e}")

if __name__ == "__main__":
    check_bp1_1_values('datos_procesados.csv')
```

### 3. Ejecución y Resolución de Dependencias
Al ejecutar el script por primera vez, se encontró un `ModuleNotFoundError` porque la librería `pandas` no estaba instalada.

Se identificó que el proyecto utiliza `uv` como gestor de paquetes, por lo que se ejecutó el siguiente comando para instalar las dependencias definidas en `pyproject.toml`:
```shell
uv sync
```
Tras la instalación, un segundo intento de ejecución volvió a fallar. Se determinó que el intérprete de Python por defecto no era el asociado al entorno virtual del proyecto donde se instalaron las dependencias.

### 4. Ejecución Exitosa
Finalmente, se ejecutó el script utilizando el intérprete de Python correcto, ubicado en el entorno virtual del proyecto:
```shell
.\.venv\Scripts\python.exe check_values.py
```

## Resultados
La ejecución del script arrojó el siguiente resultado:

```
Valores unicos en la columna BP1_1: [2 1 9]
Todos los valores en la columna BP1_1 estan dentro del rango permitido (1, 2, 9).
```

## Conclusión
La verificación fue exitosa. Se confirma que todos los valores presentes en la columna `BP1_1` del archivo `datos_procesados.csv` están contenidos en el conjunto de valores permitidos: `1`, `2` y `9`.
