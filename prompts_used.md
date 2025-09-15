# Prompts Principales para el Proyecto ENSU Yucatán

Este documento recopila los **prompts clave** que utilicé para desarrollar el pipeline de análisis de datos de la **Encuesta Nacional de Seguridad Pública Urbana (ENSU)** en Yucatán.

---

## Prompt Inicial

> **Actúa como un desarrollador Python senior y arquitecto de software.**  
> Te voy a proporcionar la documentación funcional de un proyecto para analizar datos de la ENSU en Yucatán.  
> Tu tarea es leer, comprender los requerimientos de cada fase y, a continuación, escribir el código Python completo y funcional para cada uno de los scripts necesarios.
>
> **Reglas de operación:**
> 1. **Análisis Primero:** Lee y analiza toda la documentación antes de hacer nada.  
> 2. **Proceso Guiado:** No escribirás todo el código de golpe; iremos fase por fase.  
> 3. **Propuesta y Confirmación:** Para cada fase, primero describe brevemente el script (nombre y objetivo) y pide permiso para comenzar a codificar.  
> 4. **Codificación por Demanda:** Solo después de mi confirmación escribirás el código completo en un único bloque.  
> 5. **Ciclo de Fases:** Al terminar cada fase, pregunta si podemos continuar con la siguiente.

---

## Especificaciones del Proyecto

El objetivo es procesar múltiples trimestres de datos de la ENSU para el estado de **Yucatán**, desde la extracción de datos hasta la generación de reportes visuales.

### Flujo de Datos
1. Archivos `raw` en `data/`
2. `temp/mapeo_archivos.json`
3. `temp/archivos_validados.json`
4. `temp/archivos_pendientes.json`
5. Archivos `*.csv` en `data/yucatan_processed/`
6. `reports/dataset_final_yucatan.csv`
7. `reports/reporte_percepcion_seguridad_yucatan.html`

### Columnas Requeridas
- `NOM_ENT`, `NOM_MUN`, `BP1_1`
- `NOM_CD` (opcional, usar “SIN_CIUDAD” si no existe)

### Estructura de Carpetas
`data/`, `data/yucatan_processed/`, `logs/`, `temp/`, `reports/`, `scripts/`

---

## Fases del Pipeline

### **Fase 1: Mapeo y Descubrimiento**
- **Script:** `fase1_mapeo_descubrimiento.py`
- Identifica y cataloga archivos ENSU en `data/`.
- Genera `temp/mapeo_archivos.json`.

### **Fase 2: Validación**
- **Script:** `fase2_validacion.py`
- Valida que los archivos tengan las columnas requeridas y datos de Yucatán.
- Genera `temp/archivos_validados.json`.

### **Fase 3: Verificación Incremental**
- **Script:** `fase3_verificacion_incremental.py`
- Evita reprocesar archivos ya consolidados.
- Genera `temp/archivos_pendientes.json`.

### **Fase 4: Procesamiento**
- **Script:** `fase4_procesamiento.py`
- Filtra por Yucatán, agrega datos y calcula estadísticas.
- Crea CSV por periodo en `data/yucatan_processed/`.

### **Fase 5: Consolidación de Dataset Maestro (NUEVO)**
- **Script:** `fase5_dataset.py`
- Consolida todos los CSV procesados en `reports/dataset_final_yucatan.csv`.

### **Fase 6: Reporte Visual Final (NUEVO)**
- **Script:** `fase6_reporte_final.py`
- Genera un **reporte HTML profesional** (`reports/reporte_percepcion_seguridad_yucatan.html`)
  con:
  - Gráfico de evolución estatal
  - Comparativo del periodo más reciente
  - Gráficos por municipio
  - Tabla de datos completa

---

## Ejemplo de Respuesta Inicial del Asistente

Cuando le compartí las especificaciones completas, el asistente respondió:

> *“He analizado las especificaciones del proyecto de análisis ENSU.  
> El plan consiste en un pipeline de 6 fases para procesar los datos.  
> ¿Estás listo para que desarrolle el script para la Fase 1: Mapeo y Descubrimiento?”*

---

## Notas
- El código de cada fase se escribió solo tras mi confirmación en cada paso.
- El proyecto usa **Python 3**, `pandas`, `matplotlib`, y registro de logs en cada fase.

---
