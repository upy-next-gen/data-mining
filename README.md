# data-mining

Procesamiento y reporte de datos de percepción de inseguridad.

## Uso rápido

1. Crear entorno e instalar dependencias mínimas:
   - Windows: `py -3.13 -m venv .venv && .\.venv\Scripts\activate`
   - Linux/macOS: `python3.13 -m venv .venv && source .venv/bin/activate`
   - `pip install -r requirements.txt`

2. Generar reporte HTML (auto-detecta `data/**/yucatan-inseguridad`):
   - `python generate_report.py --out ./reports/reporte.html`

3. Scripts de procesamiento existentes:
   - `python main.py` — procesa encuestas y guarda CSV en `data/yucatan-inseguridad/`.

El reporte usa Plotly (offline) y DataTables para visualizaciones y tablas interactivas.
