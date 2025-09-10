# Procesador ENSU - INEGI

Sistema de procesamiento para datos de la Encuesta Nacional de Seguridad Pública Urbana (ENSU) con enfoque en el estado de Yucatán.

## Instalación

### Requisitos previos
- Python 3.8 o superior
- PowerShell (Windows)

### Configuración del entorno

1. Crear y activar el entorno virtual:
```powershell
# Ejecutar el script de configuración
.\setup.ps1
```

2. O manualmente:
```powershell
# Crear entorno virtual
python -m venv .venv

# Activar entorno
.\.venv\Scripts\Activate.ps1

# Instalar dependencias
pip install -r requirements.txt
```

## Ejecución del pipeline completo

```powershell
# Ejecuta todo el proceso de forma secuencial
.\run_pipeline.ps1
```

## Ejecución paso a paso

```powershell
# 1. Orquestador: descubre y cataloga paquetes ENSU
python .\orquestador_ensu.py --root ".\datos completos"

# 2. Procesador: genera archivos agregados por municipio/periodo
python .\procesar_ensu_cb.py

# 3. Generador de reportes: crea visualizaciones HTML
python .\generar_reporte_evolucion.py

# 4. Verificación: realiza comprobaciones de calidad
python .\qa_checks.py
```

## Estructura de salida

```
data/
  yucatan-inseguridad/
    insumos/
      raw/                # CSVs de ENSU sin procesar
      diccionarios/       # Diccionarios de datos
    procesados/           # CSVs procesados por periodo
    reportes/             # Reportes HTML y datos tabulados
    logs/                 # Logs y archivos de inventario
```
