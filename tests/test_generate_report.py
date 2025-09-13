import os
import re
import tempfile

import pytest

import generate_report as gr


def test_smoke_generate_html():
    # Detect data directory; skip if not found or empty
    try:
        data_dir = gr.find_data_dir()
    except SystemExit:
        pytest.skip('No se encontró yucatan-inseguridad para prueba de humo')

    files = gr.detect_files(data_dir, ['*.csv', '*.json', '*.parquet'])
    if not files:
        pytest.skip('No hay archivos de datos para prueba de humo')

    with tempfile.TemporaryDirectory() as tmp:
        out_path = os.path.join(tmp, 'reporte.html')
        gr.main(['--out', out_path])

        assert os.path.exists(out_path), 'No se generó el HTML de salida'
        html = open(out_path, encoding='utf-8').read()

        # Secciones clave
        for anchor in ('resumen', 'datos', 'kpis', 'graficas', 'tablas', 'metadatos'):
            assert f'id="{anchor}"' in html

        # Interactividad presente
        assert 'window.__CHARTS__' in html
        assert 'datatable' in html

