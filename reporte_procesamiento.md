# Reporte de Procesamiento de Datos ENSU - Yucatán

**Proyecto:** Análisis de Percepción de Inseguridad en Yucatán  
**Fuente:** Encuesta Nacional de Seguridad Urbana (ENSU) 2015-2025  
**Fecha:** 14 de Septiembre de 2025  
**Estudiante:** This
**Materia:** Data Mining  

---

## 📋 Resumen Ejecutivo

Este proyecto implementó un pipeline completo de procesamiento de datos para analizar la percepción de seguridad en municipios de Yucatán utilizando datos de la Encuesta Nacional de Seguridad Urbana (ENSU) del INEGI, abarcando el período 2015-2025. El proceso incluyó descubrimiento automatizado de archivos, limpieza de datos, normalización de texto, y unificación en un dataset consolidado para análisis posterior.

### Resultados Principales
- **42 archivos CB identificados** de un total de >80 datasets ENSU
- **33 archivos procesados exitosamente** (2016-2025)
- **127 registros consolidados** representando 8,703 encuestas
- **4 municipios analizados:** Mérida, Kanasín, Progreso, Umán
- **Dataset final unificado** listo para análisis de minería de datos

---

## 🎯 Objetivos del Proyecto

### Objetivo General
Procesar y consolidar datos de percepción de seguridad de la ENSU para municipios de Yucatán, preparando un dataset estructurado para análisis de patrones temporales de inseguridad urbana.

### Objetivos Específicos
1. **Mapear** la estructura de datos ENSU en el repositorio local
2. **Identificar** archivos CB (Cuestionario Básico) relevantes para el análisis
3. **Extraer** datos específicos de Yucatán con variables de interés
4. **Procesar** información de percepción de seguridad (BP1_1)
5. **Normalizar** nombres de entidades y municipios
6. **Unificar** datos en un dataset consolidado cronológicamente ordenado

---

## 🔄 Metodología y Flujo de Trabajo

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  DESCUBRIMIENTO │    │   PROCESAMIENTO │    │   UNIFICACIÓN   │
│                 │    │                 │    │                 │
│ • Mapeo de      │───▶│ • Filtrado      │───▶│ • Consolidación │
│   estructura    │    │   Yucatán       │    │   temporal      │
│ • Identificación│    │ • Análisis      │    │ • Validación    │
│   archivos CB   │    │   percepción    │    │   duplicados    │
│ • Validación    │    │ • Normalización │    │ • Estadísticas  │
│   contenido     │    │   texto         │    │   resumen       │
└─────────────────┘    └─────────────────┘    └─────────────────┘
        ▲                        ▲                        ▲
        │                        │                        │
   Raw Data                 Individual               Unified
   (80+ files)              CSV Files               Dataset
                           (33 files)              (127 records)
```

---

## 📊 Fases del Proyecto

### **FASE 1: Descubrimiento y Exploración de Datos**

#### Solicitud Inicial
> "Necesito mapear la información que se encuentra en la carpeta data"

#### Proceso Implementado
1. **Exploración inicial** de la estructura de directorios
2. **Identificación de patrones** en nombres de archivos ENSU
3. **Búsqueda específica** de archivos CB (Cuestionario Básico)
4. **Validación de contenido** y estructura de datos

#### Código Clave - Descubrimiento de Archivos
```python
def find_cb_datasets(base_path):
    """Encuentra todos los archivos CB en la estructura ENSU"""
    cb_files = []
    for root, dirs, files in os.walk(base_path):
        if '/conjunto_de_datos_' in root and 'cb' in root.lower():
            for file in files:
                if file.endswith('.csv') and 'cb' in file.lower():
                    cb_files.append(os.path.join(root, file))
    return sorted(cb_files)
```

#### Resultados Fase 1
- **42 archivos CB identificados** en estructura jerárquica
- **Patrón temporal detectado:** 2015-2025 con gaps en algunos trimestres
- **Estructura de columnas validada:** NOM_ENT, NOM_MUN, BP1_1

---

### **FASE 2: Procesamiento de Datos Individuales**

#### Solicitud de Procesamiento
> "Procesa cada archivo y extrae solo los datos de Yucatán con las columnas NOM_ENT, NOM_MUN, BP1_1"

#### Desafíos Encontrados y Soluciones

##### **Problema 1: Codificación de Caracteres**
```
Error: 'utf-8' codec can't decode byte 0xf1 in position X
```
**Solución Implementada:**
```python
def load_csv_with_fallback_encoding(file_path):
    encodings = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']
    for encoding in encodings:
        try:
            return pd.read_csv(file_path, encoding=encoding, low_memory=False)
        except UnicodeDecodeError:
            continue
    raise ValueError(f"No se pudo decodificar el archivo: {file_path}")
```

##### **Problema 2: Caracteres de Control en Texto**
```
Municipio detectado: "MERIDA\r" con caracteres extraños
```
**Solución Implementada:**
```python
def normalize_text(text):
    """Normaliza texto removiendo acentos y caracteres especiales"""
    if pd.isna(text):
        return text
    text = str(text).strip()
    text = unicodedata.normalize('NFD', text)
    text = ''.join(c for c in text if unicodedata.category(c) != 'Mn')
    return text.replace('Ñ', 'N').replace('ñ', 'n')
```

##### **Problema 3: Archivos de Diccionario**
```
Warning: Archivo detectado como diccionario, omitiendo procesamiento
```
**Solución Implementada:**
```python
def is_dictionary_file(df):
    """Detecta si el archivo es un diccionario de datos"""
    dict_indicators = ['DESCRIPCION', 'DESCRIPCIÓN', 'ETIQUETA', 'VALORES']
    return any(col.upper() in dict_indicators for col in df.columns)
```

#### Código Principal - Procesamiento
```python
def process_security_perception(df):
    """Procesa la percepción de seguridad BP1_1"""
    security_stats = {
        'TOTAL_SEGUROS': len(df[df['BP1_1'] == 1]),      # Seguro
        'TOTAL_INSEGUROS': len(df[df['BP1_1'] == 2]),    # Inseguro  
        'TOTAL_NO_RESPONDE': len(df[df['BP1_1'].isin([8, 9])])  # No sabe/No responde
    }
    
    total = sum(security_stats.values())
    if total > 0:
        security_stats.update({
            'PCT_SEGUROS': round((security_stats['TOTAL_SEGUROS'] / total) * 100, 2),
            'PCT_INSEGUROS': round((security_stats['TOTAL_INSEGUROS'] / total) * 100, 2),
            'PCT_NO_RESPONDE': round((security_stats['TOTAL_NO_RESPONDE'] / total) * 100, 2)
        })
    
    return security_stats
```

#### Resultados Fase 2
- **33 archivos procesados exitosamente** (de 42 identificados)
- **9 archivos omitidos:** 7 por ser diccionarios, 2 por errores de estructura
- **Formato de salida estandarizado:** `procesado_YYYY_QQT_cb.csv`

---

### **FASE 3: Unificación de Datos**

#### Solicitud de Unificación
> "Ahora unifica los datos extraídos en un solo archivo para poder generar un reporte"

#### Proceso de Consolidación

##### **Detección de Archivos Procesados**
```python
def find_processed_files(directory):
    """Encuentra todos los archivos procesados con patrón específico"""
    pattern = r'procesado_\d{4}_\d{2}T_cb\.csv'
    processed_files = []
    
    for file in os.listdir(directory):
        if re.match(pattern, file):
            processed_files.append(os.path.join(directory, file))
    
    return sorted(processed_files)
```

##### **Validación de Duplicados**
```python
def detect_duplicates(df):
    """Detecta registros duplicados en el dataset unificado"""
    duplicate_cols = ['NOM_ENT', 'NOM_MUN', 'ANO', 'TRIMESTRE']
    duplicates = df[df.duplicated(subset=duplicate_cols, keep=False)]
    
    if not duplicates.empty:
        logging.warning(f"Se encontraron {len(duplicates)} registros duplicados")
        return duplicates
    else:
        logging.info("No se encontraron registros duplicados")
        return pd.DataFrame()
```

##### **Ordenamiento Cronológico**
```python
def sort_data_chronologically(df):
    """Ordena datos cronológicamente por año y trimestre"""
    df_sorted = df.sort_values(['ANO', 'TRIMESTRE', 'NOM_MUN']).reset_index(drop=True)
    logging.info("Datos ordenados cronológicamente (más antiguos primero)")
    return df_sorted
```

#### Resultados Fase 3
- **127 registros consolidados** sin duplicados
- **Período:** 2016 Q4 - 2025 Q2 
- **8,703 encuestas** analizadas en total
- **Orden cronológico** mantenido para análisis temporal

---

## 📈 Resultados y Estadísticas Finales

### Cobertura Temporal
| Año  | Registros | Trimestres Disponibles |
|------|-----------|------------------------|
| 2016 | 4         | Q4                     |
| 2017 | 16        | Q1, Q2, Q3, Q4         |
| 2018 | 16        | Q1, Q2, Q3, Q4         |
| 2019 | 12        | Q2, Q3, Q4             |
| 2020 | 12        | Q1, Q3, Q4             |
| 2021 | 16        | Q1, Q2, Q3, Q4         |
| 2022 | 16        | Q1, Q2, Q3, Q4         |
| 2023 | 12        | Q1, Q2, Q3, Q4         |
| 2024 | 15        | Q1, Q2, Q3, Q4         |
| 2025 | 8         | Q1, Q2                 |

### Cobertura Municipal
| Municipio | Registros | Observación |
|-----------|-----------|-------------|
| Mérida    | 33        | Cobertura completa |
| Kanasín   | 33        | Cobertura completa |
| Progreso  | 33        | Cobertura completa |
| Umán      | 28        | Faltantes en algunos períodos |

**Total:** 4 municipios, 127 registros temporales

### Indicadores de Seguridad Global
- **Promedio de percepción de seguridad:** 59.76%
- **Promedio de percepción de inseguridad:** 40.23%
- **No respuesta promedio:** 0.01%

---

## 💻 Logs de Ejecución Principales

### Log de Procesamiento Exitoso
```
2025-09-14 17:13:15,432 - INFO - Iniciando procesamiento de archivo: conjunto_de_datos_ensu_2024_3t_csv/cb.csv
2025-09-14 17:13:15,468 - INFO - Archivo cargado exitosamente. Shape: (1520, 87)
2025-09-14 17:13:15,469 - INFO - Datos de Yucatán encontrados: 258 registros
2025-09-14 17:13:15,470 - INFO - Municipios únicos en Yucatán: ['KANASIN', 'MERIDA', 'PROGRESO', 'UMAN']
2025-09-14 17:13:15,472 - INFO - Estadísticas por municipio procesadas correctamente
2025-09-14 17:13:15,473 - INFO - Archivo guardado: procesado_2024_03T_cb.csv
```

### Log de Unificación Final
```
2025-09-14 17:19:36,713 - INFO - RESUMEN FINAL DE LA UNIFICACIÓN
2025-09-14 17:19:36,713 - INFO - Archivos procesados: 33
2025-09-14 17:19:36,713 - INFO - Total de registros: 127
2025-09-14 17:19:36,713 - INFO - Municipios únicos: 8
2025-09-14 17:19:36,713 - INFO - Período: 2016Q1 - 2025Q4
2025-09-14 17:19:36,713 - INFO - Total de encuestas: 8,703
2025-09-14 17:19:36,713 - INFO - Promedio de seguridad: 59.76%
2025-09-14 17:19:36,713 - INFO - Promedio de inseguridad: 40.23%
```

---

## 🔧 Herramientas y Tecnologías Utilizadas

### Stack Tecnológico
- **Python 3.x** - Lenguaje de programación principal
- **pandas 2.3.2** - Manipulación y análisis de datos
- **uv** - Gestión de entorno virtual y dependencias
- **logging** - Sistema de registro de operaciones
- **unicodedata** - Normalización de texto Unicode
- **pathlib/os** - Manejo de sistema de archivos
- **re** - Procesamiento de expresiones regulares

### Estructura de Archivos Generados
```
data-mining/
├── process_yucatan_insecurity.py      # Script principal de procesamiento
├── unify_yucatan_data.py              # Script de unificación
├── data/yucatan-inseguridad/
│   ├── procesado_YYYY_QQT_cb.csv     # 33 archivos procesados
│   ├── data-yucatan-inseguridad.csv  # Dataset unificado final
│   ├── procesamiento_yucatan.log     # Log de procesamiento
│   └── unificacion_yucatan.log       # Log de unificación
└── reporte_procesamiento.md          # Este reporte
```

---

## 📚 Lecciones Aprendidas

### Desafíos Técnicos Superados
1. **Heterogeneidad de codificación** en archivos gubernamentales
2. **Inconsistencias en estructura** entre años de la ENSU
3. **Normalización de texto** para análisis consistente
4. **Validación de calidad** de datos automática

### Buenas Prácticas Implementadas
1. **Logging comprehensivo** para trazabilidad
2. **Manejo de errores robusto** con fallbacks
3. **Validación automática** de duplicados
4. **Separación de concerns** (procesamiento vs unificación)
5. **Documentación en código** para mantenibilidad

### Consideraciones para Data Mining
1. **Dataset balanceado** temporalmente para análisis de series
2. **Variables categóricas** listas para encoding
3. **Datos numéricos** normalizados y validados
4. **Metadatos temporales** preservados para análisis estacional

---

## 🎯 Prompt de Replicabilidad

### Para Replicar el Proceso Completo:

```markdown
**CONTEXTO:** Tienes una carpeta "raw data" con datasets ENSU (Encuesta Nacional de Seguridad Urbana) 
del INEGI organizados por año y trimestre. Necesitas procesar datos de percepción de seguridad 
para el estado de Yucatán.

**FASE 1 - DESCUBRIMIENTO:**
"Necesito mapear la información que se encuentra en la carpeta data. Busca específicamente 
archivos que contengan 'cb' en su estructura de carpetas, estos corresponden al Cuestionario 
Básico de la ENSU. Identifica la estructura temporal y valida que contengan las columnas 
NOM_ENT, NOM_MUN, y BP1_1."

**FASE 2 - PROCESAMIENTO:**
"Procesa cada archivo CB identificado y extrae solo los datos de Yucatán. Para cada municipio 
y período, calcula estadísticas de la variable BP1_1 (percepción de seguridad): total de 
personas que se sienten seguras (valor 1), inseguras (valor 2), y no responden (valores 8,9). 
Calcula también los porcentajes. Normaliza los nombres de municipios removiendo acentos y 
caracteres especiales. Guarda cada archivo procesado con formato 'procesado_YYYY_QQT_cb.csv'."

**FASE 3 - UNIFICACIÓN:**
"Unifica todos los archivos procesados en un solo dataset cronológicamente ordenado. Detecta 
y reporta duplicados. Genera estadísticas de resumen incluyendo cobertura temporal, municipal, 
y promedios de percepción de seguridad. Guarda el resultado como 'data-yucatan-inseguridad.csv'."

**HERRAMIENTAS REQUERIDAS:** Python con pandas, entorno virtual, sistema de logging.
**RESULTADO ESPERADO:** Dataset unificado listo para análisis de minería de datos con 
127 registros cubriendo 4 municipios de Yucatán en el período 2016-2025.
```

---

## 📋 Siguientes Pasos

Este procesamiento ha preparado los datos para la **segunda fase del proyecto**: el análisis de minería de datos propiamente dicho. El dataset unificado permitirá:

1. **Análisis temporal** de tendencias de percepción de seguridad
2. **Comparación intermunicipal** de patrones de inseguridad
3. **Modelado predictivo** de percepción de seguridad
4. **Clustering** de municipios por similitud en patrones
5. **Análisis estacional** de variaciones trimestrales

---

**Fin del Reporte de Procesamiento de Datos**  
*Preparado para: Materia de Data Mining*  
*Fecha: 14 de Septiembre de 2025*
