# Estrategia de Generación de Reportes - Evolución de Percepción de Inseguridad

## Objetivo General
Generar un reporte HTML que visualice la evolución temporal del porcentaje de percepción de inseguridad en Yucatán, tanto a nivel estatal como por municipio, utilizando los datos procesados de ENSU.

## 1. Fuente de Datos

### Archivos de Entrada
- **Ubicación**: `data/yucatan-inseguridad/procesado_*.csv`
- **Formato**: CSV con las siguientes columnas:
  - NOM_ENT: Nombre de entidad federativa
  - NOM_MUN: Nombre del municipio
  - TOTAL_REGISTROS: Total de encuestados
  - TOTAL_INSEGUROS: Encuestados que perciben inseguridad
  - PCT_INSEGUROS: Porcentaje de percepción insegura
  - AÑO: Año del dataset
  - TRIMESTRE: Trimestre (Q1, Q2, Q3, Q4)

### Filtrado de Datos
- **Entidad objetivo**: Solo registros donde NOM_ENT = "YUCATAN"
- **Variable principal**: PCT_INSEGUROS
- **Periodo**: Todos los años y trimestres disponibles

## 2. Procesamiento de Datos

### 2.1 Corrección de Trimestres

#### Problema Identificado
Algunos archivos pueden tener trimestres mal identificados (ej: múltiples Q1 en el mismo año).

#### Estrategia de Corrección
1. **Prioridad 1**: Usar información del nombre del archivo
   - Buscar patrones como `_MMYY`, `_MM_YYYY`, `cb_MMAA`
   - Convertir mes a trimestre: [01-03]→Q1, [04-06]→Q2, [07-09]→Q3, [10-12]→Q4

2. **Prioridad 2**: Usar información de la carpeta padre
   - Buscar patrones como `2017_2t`, `2018_1t`
   - Extraer trimestre del nombre de carpeta

3. **Prioridad 3**: Inferencia secuencial
   - Si hay duplicados, asignar secuencialmente
   - Ordenar por fecha de archivo o secuencia alfabética
   - Logging detallado de todas las correcciones

### 2.2 Cálculo del Promedio Estatal

#### Método: Promedio Ponderado
```python
# Para cada año-trimestre:
total_inseguros_estatal = sum(TOTAL_INSEGUROS de todos los municipios)
total_registros_estatal = sum(TOTAL_REGISTROS de todos los municipios)
pct_inseguros_estatal = (total_inseguros_estatal / total_registros_estatal) * 100
```

#### Justificación
El promedio ponderado da más peso a municipios con más encuestados, reflejando mejor la percepción general de la población.

### 2.3 Manejo de Datos Faltantes

#### Estrategia
- **Identificación**: Detectar municipios que no aparecen en todos los trimestres
- **Visualización**: Dejar gaps (líneas discontinuas) en los gráficos
- **Logging**: Registrar todos los datos faltantes con detalle:
  - Municipio
  - Trimestres faltantes
  - Posible causa (municipio nuevo, sin datos, etc.)

## 3. Generación de Visualizaciones

### 3.1 Especificaciones de Gráficos

#### Configuración General
- **Tipo**: Gráfico de líneas
- **Dimensiones**: 800x500 píxeles
- **Librería**: matplotlib con backend no interactivo
- **Formato**: PNG embebido en base64 en HTML

#### Elementos del Gráfico
- **Eje X**: Tiempo (formato: YYYY-Q#)
- **Eje Y**: Porcentaje de inseguridad (0-100%)
- **Título**: Claro y descriptivo
- **Grid**: Activado para mejor lectura
- **Leyenda**: Solo si hay múltiples líneas

### 3.2 Gráfico Estatal

#### Características
- **Título**: "Evolución de la Percepción de Inseguridad en Yucatán"
- **Línea**: Grosor mayor (2-3 pts)
- **Color**: Azul marino o según nivel promedio
- **Marcadores**: En cada punto de datos

### 3.3 Gráficos Municipales

#### Características
- **Título**: "Municipio: [NOMBRE] - Evolución de Percepción de Inseguridad"
- **Ordenamiento**: Por promedio histórico de inseguridad (mayor a menor)
- **Color de línea**: Según nivel de inseguridad
  - Verde: < 30%
  - Amarillo/Naranja: 30-60%
  - Rojo: > 60%

## 4. Estructura del Reporte HTML

### 4.1 Diseño del Documento

```html
<!DOCTYPE html>
<html>
<head>
    <title>Reporte de Evolución - Percepción de Inseguridad en Yucatán</title>
    <style>
        /* CSS para formato profesional */
        body { font-family: Arial, sans-serif; margin: 40px; }
        h1, h2 { color: #333; }
        .grafico { margin: 30px 0; text-align: center; }
        .tabla { margin: 30px auto; border-collapse: collapse; }
        .metadata { background: #f0f0f0; padding: 10px; }
    </style>
</head>
<body>
    <!-- 1. Encabezado -->
    <h1>Evolución de la Percepción de Inseguridad en Yucatán</h1>
    <div class="metadata">
        <p>Fecha de generación: [FECHA]</p>
        <p>Periodo analizado: [INICIO] - [FIN]</p>
        <p>Fuente: Encuesta Nacional de Seguridad Urbana (ENSU)</p>
    </div>
    
    <!-- 2. Índice de contenidos -->
    <h2>Contenido</h2>
    <ul>
        <li><a href="#estatal">Promedio Estatal</a></li>
        <li><a href="#municipios">Análisis por Municipio</a></li>
        <li><a href="#tabla">Tabla de Datos Completa</a></li>
        <li><a href="#notas">Notas Metodológicas</a></li>
    </ul>
    
    <!-- 3. Gráfico Estatal -->
    <h2 id="estatal">Promedio Estatal de Yucatán</h2>
    <div class="grafico">
        <img src="data:image/png;base64,[GRAFICO_ESTATAL_BASE64]">
    </div>
    
    <!-- 4. Gráficos Municipales -->
    <h2 id="municipios">Análisis por Municipio</h2>
    <!-- Ordenados por nivel de inseguridad -->
    <div class="grafico">
        <h3>Municipio: [NOMBRE]</h3>
        <p>Promedio histórico: [XX.X]%</p>
        <img src="data:image/png;base64,[GRAFICO_MUNICIPAL_BASE64]">
    </div>
    <!-- Repetir para cada municipio -->
    
    <!-- 5. Tabla de Datos -->
    <h2 id="tabla">Tabla de Datos Completa</h2>
    <table class="tabla">
        <thead>
            <tr>
                <th>Municipio</th>
                <th>2016-Q2</th>
                <th>2017-Q1</th>
                <!-- Columnas para cada año-trimestre -->
            </tr>
        </thead>
        <tbody>
            <!-- Filas con datos -->
        </tbody>
    </table>
    
    <!-- 6. Notas -->
    <h2 id="notas">Notas Metodológicas</h2>
    <ul>
        <li>Datos faltantes se muestran como gaps en las líneas</li>
        <li>El promedio estatal usa ponderación por número de encuestados</li>
        <li>Trimestres corregidos: [LISTA]</li>
    </ul>
</body>
</html>
```

### 4.2 Contenido de la Tabla

#### Estructura
- **Filas**: Un municipio por fila + fila para promedio estatal
- **Columnas**: Un año-trimestre por columna
- **Celdas**: Valor de PCT_INSEGUROS o vacío si no hay datos
- **Formato**: Valores con 1 decimal (XX.X%)
- **Colores de fondo**: Opcional, escala de calor

## 5. Sistema de Logging

### 5.1 Archivo de Log
- **Ubicación**: `data/yucatan-inseguridad/reportes/log_analisis_yucatan.log`
- **Formato**: Similar al logging anterior (timestamp - nivel - mensaje)

### 5.2 Información a Registrar

#### Inicio del Proceso
```
YYYY-MM-DD HH:MM:SS - INFO - Iniciando generación de reporte de evolución
YYYY-MM-DD HH:MM:SS - INFO - Archivos a procesar: [lista]
```

#### Corrección de Trimestres
```
YYYY-MM-DD HH:MM:SS - WARNING - Trimestre duplicado detectado: [archivo]
YYYY-MM-DD HH:MM:SS - INFO - Corregido: [archivo] de Q1 a Q2 (inferencia secuencial)
```

#### Datos Faltantes
```
YYYY-MM-DD HH:MM:SS - INFO - Municipio [NOMBRE] - Datos faltantes en: [trimestres]
YYYY-MM-DD HH:MM:SS - INFO - Total de gaps detectados: [N]
```

#### Estadísticas
```
YYYY-MM-DD HH:MM:SS - INFO - Municipios procesados: [N]
YYYY-MM-DD HH:MM:SS - INFO - Periodo cubierto: [INICIO] a [FIN]
YYYY-MM-DD HH:MM:SS - INFO - Promedio estatal general: [XX.X]%
```

## 6. Implementación Técnica

### 6.1 Dependencias Python
```python
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')  # Backend no interactivo
import base64
from io import BytesIO
import logging
from datetime import datetime
import re
from pathlib import Path
```

### 6.2 Estructura del Script

```python
def main():
    # 1. Configurar logging
    configurar_logging()
    
    # 2. Cargar todos los CSV procesados
    datos = cargar_datos_yucatan()
    
    # 3. Corregir trimestres duplicados
    datos = corregir_trimestres(datos)
    
    # 4. Calcular promedio estatal ponderado
    promedio_estatal = calcular_promedio_estatal(datos)
    
    # 5. Identificar y loggear datos faltantes
    gaps = identificar_gaps(datos)
    
    # 6. Generar gráfico estatal
    grafico_estatal = generar_grafico_estatal(promedio_estatal)
    
    # 7. Generar gráficos municipales
    graficos_municipales = generar_graficos_municipales(datos)
    
    # 8. Crear tabla de datos
    tabla = crear_tabla_completa(datos)
    
    # 9. Generar HTML
    generar_html(grafico_estatal, graficos_municipales, tabla)
    
    # 10. Resumen final en log
    generar_resumen_log()
```

### 6.3 Funciones Clave

#### Corrección de Trimestres
```python
def corregir_trimestres(df):
    """
    Detecta y corrige trimestres duplicados
    Prioridad: 
    1. Información del nombre del archivo
    2. Inferencia secuencial
    """
    # Agrupar por año
    for año in df['AÑO'].unique():
        datos_año = df[df['AÑO'] == año]
        trimestres = datos_año['TRIMESTRE'].value_counts()
        
        if any(trimestres > 1):
            # Hay duplicados, corregir secuencialmente
            logging.warning(f"Trimestres duplicados en año {año}")
            # Lógica de corrección...
    
    return df
```

#### Cálculo del Promedio Estatal
```python
def calcular_promedio_estatal(df):
    """
    Calcula el promedio ponderado por trimestre
    """
    resultado = []
    
    for periodo in df['PERIODO'].unique():
        datos_periodo = df[df['PERIODO'] == periodo]
        
        total_inseguros = datos_periodo['TOTAL_INSEGUROS'].sum()
        total_registros = datos_periodo['TOTAL_REGISTROS'].sum()
        
        if total_registros > 0:
            pct_inseguros = (total_inseguros / total_registros) * 100
            resultado.append({
                'PERIODO': periodo,
                'PCT_INSEGUROS': pct_inseguros,
                'TOTAL_REGISTROS': total_registros
            })
    
    return pd.DataFrame(resultado)
```

## 7. Validación y Control de Calidad

### 7.1 Verificaciones Pre-procesamiento
- [ ] Confirmar que existen archivos de Yucatán
- [ ] Validar formato de columnas esperadas
- [ ] Verificar rangos válidos de porcentajes (0-100)

### 7.2 Verificaciones Post-procesamiento
- [ ] Todos los municipios tienen al menos un punto de datos
- [ ] No hay valores negativos o mayores a 100%
- [ ] El HTML se genera correctamente
- [ ] Los gráficos son legibles
- [ ] La tabla contiene todos los datos

### 7.3 Validación Manual
- [ ] Revisar trimestres corregidos en el log
- [ ] Verificar que los promedios estatales son coherentes
- [ ] Confirmar que los gaps se muestran correctamente

## 8. Archivos de Salida

### Estructura de Carpetas
```
data/yucatan-inseguridad/
├── reportes/
│   ├── reporte_yucatan_evolucion.html
│   ├── log_analisis_yucatan.log
│   └── datos_tabulados_yucatan.csv (opcional)
```

### Nomenclatura con Timestamp (Opcional)
Si se desea mantener histórico:
- `reporte_yucatan_evolucion_YYYYMMDD_HHMMSS.html`
- `log_analisis_yucatan_YYYYMMDD_HHMMSS.log`

## 9. Consideraciones Especiales

### 9.1 Rendimiento
- Para archivos HTML grandes, considerar compresión
- Optimizar imágenes base64 (calidad vs tamaño)
- Procesar datos en chunks si hay memoria limitada

### 9.2 Usabilidad
- Incluir navegación interna con anclas HTML
- Asegurar que el HTML es responsive
- Tabla con scroll horizontal si hay muchas columnas

### 9.3 Extensibilidad
- Código modular para agregar otros estados fácilmente
- Parámetros configurables (colores, tamaños, etc.)
- Posibilidad de generar otros formatos (PDF, Excel)

## 10. Ejecución

### Comando
```bash
uv run python generar_reporte_evolucion.py
```

### Parámetros Opcionales (si se implementan)
```bash
uv run python generar_reporte_evolucion.py \
    --estado YUCATAN \
    --variable PCT_INSEGUROS \
    --output reportes/
```

## 11. Mejoras Futuras

### Corto Plazo
- Agregar línea de tendencia
- Incluir promedio móvil
- Marcar eventos relevantes

### Mediano Plazo
- Dashboard interactivo con filtros
- Comparación entre estados
- Análisis de correlación con otros indicadores

### Largo Plazo
- Predicción con series de tiempo
- Análisis geoespacial con mapas
- API para consultas dinámicas