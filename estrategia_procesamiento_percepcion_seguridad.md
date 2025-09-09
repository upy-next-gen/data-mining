# Estrategia de Procesamiento - Percepción de Seguridad ENSU

## Objetivo General
Procesar datasets de la Encuesta Nacional de Seguridad Urbana (ENSU) para generar estadísticas agregadas de percepción de seguridad a nivel municipal.

## 1. Identificación de Datasets

### Criterios de Búsqueda
- **Ubicación**: Carpeta `data/`
- **Patrón de nombre**: Archivos que contengan la cadena "cb" en su nombre
- **Formato**: Archivos CSV

### Columnas Requeridas
Los datasets deben contener las siguientes columnas:
- `NOM_ENT`: Nombre de la entidad federativa (string)
- `NOM_MUN`: Nombre del municipio (string)
- `BP1_1`: Percepción de seguridad (valores: 1, 2, 9)

### Valores de BP1_1
- `1` = Seguro
- `2` = Inseguro
- `9` = No responde

## 2. Validación de Datos

### Validaciones Requeridas
1. **Existencia de columnas**: Verificar que existan NOM_ENT, NOM_MUN, BP1_1
2. **Valores válidos**: BP1_1 debe contener solo valores en [1, 2, 9]
3. **Registros excluidos**:
   - Registros con BP1_1 fuera del rango válido
   - Registros con BP1_1 null o vacío

### Estrategia de Logging
- Registrar todos los casos de exclusión
- Mantener contadores de registros procesados vs excluidos
- Generar archivo de log detallado con timestamps

## 3. Procesamiento de Datos

### Normalización de Strings
Aplicar a NOM_ENT y NOM_MUN:
1. Convertir a MAYÚSCULAS
2. Eliminar acentos:
   - á → A, é → E, í → I, ó → O, ú → U
3. Convertir Ñ → N
4. Eliminar espacios extras

### Agrupación
- Agrupar registros por: `NOM_ENT` + `NOM_MUN`
- Ordenamiento: Alfabético por NOM_ENT, luego por NOM_MUN

### Cálculos por Grupo
Para cada combinación NOM_ENT + NOM_MUN, calcular:

| Campo | Descripción | Fórmula |
|-------|-------------|---------|
| TOTAL_REGISTROS | Total de registros válidos | COUNT(registros con BP1_1 ∈ [1,2,9]) |
| TOTAL_SEGUROS | Registros con percepción segura | COUNT(BP1_1 = 1) |
| TOTAL_INSEGUROS | Registros con percepción insegura | COUNT(BP1_1 = 2) |
| TOTAL_NO_RESPONDE | Registros sin respuesta | COUNT(BP1_1 = 9) |
| PCT_SEGUROS | Porcentaje de percepción segura | (TOTAL_SEGUROS / TOTAL_REGISTROS) × 100 |
| PCT_INSEGUROS | Porcentaje de percepción insegura | (TOTAL_INSEGUROS / TOTAL_REGISTROS) × 100 |
| PCT_NO_RESPONDE | Porcentaje sin respuesta | (TOTAL_NO_RESPONDE / TOTAL_REGISTROS) × 100 |

### Extracción de Metadata
Del nombre del archivo, extraer:
- **AÑO**: Año del dataset
- **TRIMESTRE**: Trimestre del dataset

#### Mapeo de Trimestres
- Meses 01-03 → Trimestre 1 (Q1)
- Meses 04-06 → Trimestre 2 (Q2)
- Meses 07-09 → Trimestre 3 (Q3)
- Meses 10-12 → Trimestre 4 (Q4)

**Ejemplo**: `cb_0322` → Mes 03, Año 2022 → 2022, Q1

Si no se puede identificar: usar "DESCONOCIDO"

## 4. Estructura de Salida

### Columnas del Dataset Resultante
1. NOM_ENT
2. NOM_MUN
3. TOTAL_REGISTROS
4. TOTAL_SEGUROS
5. TOTAL_INSEGUROS
6. TOTAL_NO_RESPONDE
7. PCT_SEGUROS (formato: 0-100 con decimales)
8. PCT_INSEGUROS (formato: 0-100 con decimales)
9. PCT_NO_RESPONDE (formato: 0-100 con decimales)
10. AÑO
11. TRIMESTRE

### Archivos de Salida
- **Ubicación**: `data/yucatan-inseguridad/`
- **Nomenclatura**: `procesado_YYYY_QT_cb.csv`
  - YYYY: Año (4 dígitos)
  - QT: Q + número de trimestre (Q1, Q2, Q3, Q4)
- **Encoding**: UTF-8
- **Formato**: CSV con headers

## 5. Sistema de Logging

### Archivo de Log
- **Nombre**: `procesamiento_log_YYYYMMDD_HHMMSS.log`
- **Ubicación**: `data/yucatan-inseguridad/logs/`

### Información a Registrar

#### Por Dataset:
- Timestamp de inicio/fin de procesamiento
- Nombre del archivo original
- Validación de columnas (PASS/FAIL)
- Total de registros en archivo original
- Total de registros válidos procesados
- Total de registros excluidos por categoría:
  - BP1_1 null/vacío
  - BP1_1 fuera de rango
- Año y trimestre identificados (o "DESCONOCIDO")
- Ruta del archivo de salida generado

#### Estadísticas Globales:
- Total de archivos encontrados
- Total de archivos procesados exitosamente
- Total de archivos rechazados (y razones)
- Tiempo total de procesamiento

### Niveles de Log
- **INFO**: Operaciones normales
- **WARNING**: Registros excluidos, valores no esperados
- **ERROR**: Archivos sin columnas requeridas, errores de procesamiento

## 6. Implementación Técnica

### Tecnologías Sugeridas
- **Lenguaje**: Python 3.8+
- **Librerías principales**:
  - pandas: Manipulación de datos
  - unicodedata: Normalización de caracteres
  - logging: Sistema de logs
  - pathlib: Manejo de rutas
  - re: Expresiones regulares para extraer año/trimestre

### Estructura del Script

```python
# Pseudo-estructura del script principal

def main():
    # 1. Configurar logging
    setup_logging()
    
    # 2. Buscar archivos CB
    archivos_cb = buscar_archivos_cb("data/")
    
    # 3. Para cada archivo:
    for archivo in archivos_cb:
        try:
            # a. Validar columnas
            if not validar_columnas(archivo):
                log_error(f"Archivo sin columnas requeridas: {archivo}")
                continue
            
            # b. Cargar y filtrar datos
            df = cargar_y_filtrar_datos(archivo)
            
            # c. Normalizar strings
            df = normalizar_strings(df)
            
            # d. Agrupar y calcular estadísticas
            df_agregado = calcular_estadisticas(df)
            
            # e. Agregar metadata (año, trimestre)
            df_agregado = agregar_metadata(df_agregado, archivo)
            
            # f. Guardar resultado
            guardar_resultado(df_agregado, archivo)
            
            log_info(f"Procesado exitosamente: {archivo}")
            
        except Exception as e:
            log_error(f"Error procesando {archivo}: {str(e)}")
    
    # 4. Generar resumen final
    generar_resumen_final()
```

### Funciones Clave

#### Normalización de Strings
```python
def normalizar_string(texto):
    # 1. Convertir a mayúsculas
    # 2. Eliminar acentos
    # 3. Convertir Ñ a N
    # 4. Limpiar espacios
    return texto_normalizado
```

#### Extracción de Año y Trimestre
```python
def extraer_año_trimestre(nombre_archivo):
    # Buscar patrón como "cb_MMAA" o variaciones
    # Retornar (año, trimestre) o ("DESCONOCIDO", "DESCONOCIDO")
    return año, trimestre
```

## 7. Consideraciones Especiales

### Manejo de Memoria
- Procesar archivos grandes en chunks si es necesario
- Liberar memoria después de procesar cada archivo

### Validación de Resultados
- Verificar que la suma de porcentajes sea 100%
- Verificar que no haya duplicados en NOM_ENT + NOM_MUN por archivo

### Respaldo
- Mantener archivos originales intactos
- Toda la salida en carpeta separada

## 8. Entregables

1. **Scripts de procesamiento** (Python)
2. **Archivos CSV procesados** en `data/yucatan-inseguridad/`
3. **Archivo de log detallado** con toda la trazabilidad
4. **Resumen ejecutivo** con estadísticas generales del procesamiento

## 9. Cronograma de Ejecución

1. **Fase 1**: Exploración y validación de datasets (identificar cuáles cumplen criterios)
2. **Fase 2**: Desarrollo del script de procesamiento
3. **Fase 3**: Ejecución y generación de resultados
4. **Fase 4**: Validación de resultados y generación de reportes

## 10. Control de Calidad

### Verificaciones Post-Procesamiento
- [ ] Todos los archivos CB fueron evaluados
- [ ] Los archivos de salida tienen el formato correcto
- [ ] Los porcentajes suman 100% en cada registro
- [ ] El log captura todos los eventos relevantes
- [ ] No hay pérdida de datos válidos
- [ ] La normalización de strings es consistente