#!/usr/bin/env python3
"""
Fase 5: Reporte Final
Genera un reporte completo del procesamiento y un dataset maestro
"""

import json
import pandas as pd
import logging
from pathlib import Path
from datetime import datetime
import os
from glob import glob
import plotly.express as px

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/fase5_reporte.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def cargar_datasets_procesados():
    """
    Carga todos los datasets procesados de Yucatán
    """
    logger.info("Cargando datasets procesados...")
    archivos_csv = glob('data/yucatan_processed/*.csv')
    
    datasets = []
    for archivo in sorted(archivos_csv):
        logger.info(f"  - Cargando: {os.path.basename(archivo)}")
        df = pd.read_csv(archivo)
        datasets.append(df)
    
    if datasets:
        df_completo = pd.concat(datasets, ignore_index=True)
        logger.info(f"Total de registros combinados: {len(df_completo)}")
        # Extraer AÑO y TRIMESTRE de la columna PERIODO
        df_completo[['AÑO', 'TRIMESTRE']] = df_completo['PERIODO'].str.split('_Q', expand=True)
        df_completo['AÑO'] = pd.to_numeric(df_completo['AÑO'])
        df_completo['TRIMESTRE'] = pd.to_numeric(df_completo['TRIMESTRE'])
        logger.info("Columnas 'AÑO' y 'TRIMESTRE' creadas a partir de 'PERIODO'.")
        return df_completo
    else:
        logger.warning("No se encontraron datasets procesados")
        return pd.DataFrame()

def generar_estadisticas_globales(df):
    """
    Genera estadísticas globales del dataset completo
    """
    if df.empty:
        return {}
    
    estadisticas = {
        'total_registros': int(len(df)),
        'años_cubiertos': [int(x) for x in sorted(df['AÑO'].unique().tolist())],
        'trimestres_totales': int(len(df[['AÑO', 'TRIMESTRE']].drop_duplicates())),
        'municipios_unicos': int(df['NOM_MUN'].nunique()),
        'ciudades_unicas': int(df['NOM_CD'].nunique()),
        
        # Estadísticas de percepción
        'total_respuestas_seguro': int(df['TOTAL_SEGUROS'].sum()),
        'total_respuestas_inseguro': int(df['TOTAL_INSEGUROS'].sum()),
        'total_no_responde': int(df['TOTAL_NO_RESPONDE'].sum()),
        
        # Promedios generales
        'promedio_pct_seguros': round(df['PORCENTAJE_SEGUROS'].mean(), 2),
        'promedio_pct_inseguros': round(df['PORCENTAJE_INSEGUROS'].mean(), 2),
        
        # Por año
        'estadisticas_por_año': []
    }
    
    # Calcular estadísticas por año
    for año in sorted(df['AÑO'].unique()):
        df_año = df[df['AÑO'] == año]
        estadisticas['estadisticas_por_año'].append({
            'año': int(año),
            'trimestres': int(len(df_año['TRIMESTRE'].unique())),
            'registros': int(len(df_año)),
            'promedio_pct_seguros': round(df_año['PORCENTAJE_SEGUROS'].mean(), 2),
            'promedio_pct_inseguros': round(df_año['PORCENTAJE_INSEGUROS'].mean(), 2)
        })
    
    # --- NUEVA LÓGICA DE RANKING BASADA EN EL PERÍODO MÁS RECIENTE ---
    latest_year = df['AÑO'].max()
    latest_trimestre = df[df['AÑO'] == latest_year]['TRIMESTRE'].max()
    df_latest = df[(df['AÑO'] == latest_year) & (df['TRIMESTRE'] == latest_trimestre)]

    if not df_latest.empty:
        mas_seguro_info = df_latest.loc[df_latest['PORCENTAJE_SEGUROS'].idxmax()]
        mas_inseguro_info = df_latest.loc[df_latest['PORCENTAJE_INSEGUROS'].idxmax()]

        estadisticas['municipio_mas_seguro'] = {
            'nombre': mas_seguro_info['NOM_MUN'],
            'pct_promedio_seguros': float(mas_seguro_info['PORCENTAJE_SEGUROS'])
        }
        estadisticas['municipio_mas_inseguro'] = {
            'nombre': mas_inseguro_info['NOM_MUN'],
            'pct_promedio_inseguros': float(mas_inseguro_info['PORCENTAJE_INSEGUROS'])
        }
    else:
        estadisticas['municipio_mas_seguro'] = {'nombre': 'N/A', 'pct_promedio_seguros': 0}
        estadisticas['municipio_mas_inseguro'] = {'nombre': 'N/A', 'pct_promedio_inseguros': 0}

    return estadisticas

def generar_reporte_html(df_completo, estadisticas, resumen_procesamiento):
    """
    Genera un reporte HTML con todas las estadísticas
    """
    html_content = """
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Reporte ENSU Yucatán - Percepción de Seguridad</title>
    <style>
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 20px;
            background-color: #f5f5f5;
        }
        .container {
            max-width: 1200px;
            margin: 0 auto;
            background: white;
            padding: 30px;
            border-radius: 10px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }
        h1 {
            color: #2c3e50;
            border-bottom: 3px solid #3498db;
            padding-bottom: 10px;
        }
        h2 {
            color: #34495e;
            margin-top: 30px;
        }
        .stat-card {
            background: #ecf0f1;
            padding: 15px;
            border-radius: 5px;
            margin: 10px 0;
        }
        .stat-label {
            font-weight: bold;
            color: #7f8c8d;
        }
        .stat-value {
            font-size: 24px;
            color: #2c3e50;
            margin-top: 5px;
        }
        .grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin: 20px 0;
        }
        table {
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
        }
        th {
            background-color: #3498db;
            color: white;
            padding: 10px;
            text-align: left;
        }
        td {
            padding: 10px;
            border-bottom: 1px solid #ecf0f1;
        }
        tr:hover {
            background-color: #f8f9fa;
        }
        .success {
            color: #27ae60;
        }
        .warning {
            color: #f39c12;
        }
        .info {
            color: #3498db;
        }
        .footer {
            margin-top: 40px;
            padding-top: 20px;
            border-top: 1px solid #ecf0f1;
            text-align: center;
            color: #7f8c8d;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>📊 Reporte ENSU Yucatán - Percepción de Seguridad</h1>
        <p>Fecha de generación: {{FECHA_GENERACION}}</p>
        
        <h2>📈 Resumen del Procesamiento</h2>
        <div class="grid">
            <div class="stat-card">
                <div class="stat-label">Archivos Procesados</div>
                <div class="stat-value success">{{ARCHIVOS_EXITOSOS}}/{{TOTAL_ARCHIVOS}}</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Total Registros Yucatán</div>
                <div class="stat-value info">{{TOTAL_REGISTROS}}</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Años Cubiertos</div>
                <div class="stat-value">{{AÑOS_CUBIERTOS}}</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Trimestres Procesados</div>
                <div class="stat-value">{{TRIMESTRES_TOTALES}}</div>
            </div>
        </div>
        
        <h2>🔍 Percepción General de Seguridad</h2>
        <div class="grid">
            <div class="stat-card">
                <div class="stat-label">Promedio % Seguro</div>
                <div class="stat-value success">{{PROMEDIO_PCT_SEGUROS}}%</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Promedio % Inseguro</div>
                <div class="stat-value warning">{{PROMEDIO_PCT_INSEGUROS}}%</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Municipios Analizados</div>
                <div class="stat-value">{{MUNICIPIOS_UNICOS}}</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Ciudades Analizadas</div>
                <div class="stat-value">{{CIUDADES_UNICAS}}</div>
            </div>
        </div>
        
        <h2>📅 Evolución por Año</h2>
        <table>
            <thead>
                <tr>
                    <th>Año</th>
                    <th>Trimestres</th>
                    <th>Registros</th>
                    <th>% Promedio Seguro</th>
                    <th>% Promedio Inseguro</th>
                </tr>
            </thead>
            <tbody>
                {{TABLA_AÑOS}}
            </tbody>
        </table>

        <h2>📊 Evolución de Inseguridad por Municipio (Gráfica de Puntos)</h2>
        <div class="chart-container">
            {{GRAFICA_PUNTOS}}
        </div>
        
        <h2>🏆 Rankings de Municipios</h2>
        <div class="grid">
            <div class="stat-card">
                <div class="stat-label">Municipio Más Seguro</div>
                <div class="stat-value success">{{MUNICIPIO_MAS_SEGURO}}</div>
                <small>{{PCT_MAS_SEGURO}}% se sienten seguros</small>
            </div>
            <div class="stat-card">
                <div class="stat-label">Municipio Más Inseguro</div>
                <div class="stat-value warning">{{MUNICIPIO_MAS_INSEGURO}}</div>
                <small>{{PCT_MAS_INSEGURO}}% se sienten inseguros</small>
            </div>
        </div>
        
        <h2>📁 Archivos Procesados</h2>
        <table>
            <thead>
                <tr>
                    <th>Archivo Original</th>
                    <th>Año</th>
                    <th>Trimestre</th>
                    <th>Registros Yucatán</th>
                    <th>Municipios</th>
                </tr>
            </thead>
            <tbody>
                {{TABLA_ARCHIVOS}}
            </tbody>
        </table>
        
        <div class="footer">
            <p>Pipeline de Procesamiento ENSU - Universidad Politécnica de Yucatán</p>
            <p>Generado automáticamente el {{FECHA_GENERACION}}</p>
        </div>
    </div>
</body>
</html>
"""
    
    # --- INICIO: Generar gráfica de puntos ---
    plot_html = ""
    if not df_completo.empty:
        # Crear una columna de fecha para el eje X
        df_completo['FECHA'] = df_completo.apply(lambda row: f"{row['AÑO']}-{(row['TRIMESTRE'] - 1) * 3 + 1:02d}-01", axis=1)
        df_completo['FECHA'] = pd.to_datetime(df_completo['FECHA'])

        fig = px.line(
            df_completo,
            x='FECHA',
            y='PORCENTAJE_INSEGUROS',
            color='NOM_MUN',
            title='Evolución de Percepción de Inseguridad por Municipio',
            labels={'PORCENTAJE_INSEGUROS': '% de Percepción de Inseguridad', 'FECHA': 'Fecha', 'NOM_MUN': 'Municipio'},
            markers=True
        )
        fig.update_traces(marker=dict(size=10, line=dict(width=1, color='DarkSlateGrey')), selector=dict(mode='markers'))
        plot_html = fig.to_html(full_html=False, include_plotlyjs='cdn')
    # --- FIN: Generar gráfica de puntos ---

    # Preparar datos para el template
    fecha_generacion = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # Tabla de años
    tabla_años = ""
    for año_stat in estadisticas.get('estadisticas_por_año', []):
        tabla_años += f"""
            <tr>
                <td>{año_stat['año']}</td>
                <td>{año_stat['trimestres']}</td>
                <td>{año_stat['registros']}</td>
                <td class="success">{año_stat['promedio_pct_seguros']}%</td>
                <td class="warning">{año_stat['promedio_pct_inseguros']}%</td>
            </tr>
        """
    
    # Tabla de archivos procesados
    tabla_archivos = ""
    for resultado in resumen_procesamiento.get('resultados', []):
        if resultado.get('exito'):
            # Extraer año y trimestre del período para la tabla
            periodo_split = resultado['periodo'].split('_Q')
            año = periodo_split[0]
            trimestre = periodo_split[1]
            tabla_archivos += f"""
                <tr>
                    <td>{os.path.basename(resultado['archivo_origen'])}</td>
                    <td>{año}</td>
                    <td>T{trimestre}</td>
                    <td>{resultado['estadisticas']['registros_yucatan']}</td>
                    <td>{resultado['estadisticas']['municipios']}</td>
                </tr>
            """
    
    # Reemplazar placeholders
    html_content = html_content.replace('{{FECHA_GENERACION}}', fecha_generacion)
    html_content = html_content.replace('{{ARCHIVOS_EXITOSOS}}', str(resumen_procesamiento.get('archivos_procesados', 0)))
    html_content = html_content.replace('{{TOTAL_ARCHIVOS}}', str(resumen_procesamiento.get('total_archivos', 0)))
    html_content = html_content.replace('{{TOTAL_REGISTROS}}', str(estadisticas.get('total_registros', 0)))
    html_content = html_content.replace('{{AÑOS_CUBIERTOS}}', f"{min(estadisticas.get('años_cubiertos', [2016]))}-{max(estadisticas.get('años_cubiertos', [2025]))}")
    html_content = html_content.replace('{{TRIMESTRES_TOTALES}}', str(estadisticas.get('trimestres_totales', 0)))
    html_content = html_content.replace('{{PROMEDIO_PCT_SEGUROS}}', str(estadisticas.get('promedio_pct_seguros', 0)))
    html_content = html_content.replace('{{PROMEDIO_PCT_INSEGUROS}}', str(estadisticas.get('promedio_pct_inseguros', 0)))
    html_content = html_content.replace('{{MUNICIPIOS_UNICOS}}', str(estadisticas.get('municipios_unicos', 0)))
    html_content = html_content.replace('{{CIUDADES_UNICAS}}', str(estadisticas.get('ciudades_unicas', 0)))
    html_content = html_content.replace('{{TABLA_AÑOS}}', tabla_años)
    html_content = html_content.replace('{{TABLA_ARCHIVOS}}', tabla_archivos)
    html_content = html_content.replace('{{GRAFICA_PUNTOS}}', plot_html)

    # Rankings
    municipio_seguro = estadisticas.get('municipio_mas_seguro', {})
    html_content = html_content.replace('{{MUNICIPIO_MAS_SEGURO}}', municipio_seguro.get('nombre', 'N/A'))
    html_content = html_content.replace('{{PCT_MAS_SEGURO}}', str(municipio_seguro.get('pct_promedio_seguros', 0)))
    
    municipio_inseguro = estadisticas.get('municipio_mas_inseguro', {})
    html_content = html_content.replace('{{MUNICIPIO_MAS_INSEGURO}}', municipio_inseguro.get('nombre', 'N/A'))
    html_content = html_content.replace('{{PCT_MAS_INSEGURO}}', str(municipio_inseguro.get('pct_promedio_inseguros', 0)))
    
    return html_content

def main():
    """
    Función principal de la Fase 5
    """
    logger.info("=== FASE 5: GENERACIÓN DE REPORTE FINAL ===")
    
    # Cargar resumen de procesamiento
    resumen_path = 'temp/procesamiento_resultados.json'
    if not Path(resumen_path).exists():
        logger.error(f"No se encontró el resumen de procesamiento en {resumen_path}")
        logger.error("Ejecute primero la Fase 4")
        return False
    
    with open(resumen_path, 'r', encoding='utf-8') as f:
        resumen_procesamiento = json.load(f)
    
    # Cargar todos los datasets procesados
    df_completo = cargar_datasets_procesados()
    
    if df_completo.empty:
        logger.error("No hay datos para generar el reporte")
        return False
    
    # Generar estadísticas globales
    logger.info("Generando estadísticas globales...")
    estadisticas = generar_estadisticas_globales(df_completo)
    
    # Guardar dataset maestro
    logger.info("Guardando dataset maestro...")
    df_completo.to_csv('reports/dataset_maestro_yucatan.csv', index=False)
    logger.info(f"  - Dataset maestro guardado: reports/dataset_maestro_yucatan.csv")
    
    # Generar reporte HTML
    logger.info("Generando reporte HTML...")
    html_content = generar_reporte_html(df_completo, estadisticas, resumen_procesamiento)
    
    # Crear directorio de reportes si no existe
    os.makedirs('reports', exist_ok=True)
    
    # Guardar reporte HTML
    html_path = 'reports/reporte_ensu_yucatan.html'
    with open(html_path, 'w', encoding='utf-8') as f:
        f.write(html_content)
    logger.info(f"  - Reporte HTML guardado: {html_path}")
    
    # Guardar estadísticas en JSON
    json_path = 'reports/estadisticas_finales.json'
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump({
            'fecha_generacion': datetime.now().isoformat(),
            'estadisticas_globales': estadisticas,
            'resumen_procesamiento': resumen_procesamiento
        }, f, indent=2, ensure_ascii=False)
    logger.info(f"  - Estadísticas JSON guardadas: {json_path}")
    
    # Resumen final
    logger.info("\n=== RESUMEN FINAL ===")
    logger.info(f"Total de registros procesados: {estadisticas['total_registros']}")
    logger.info(f"Años cubiertos: {min(estadisticas['años_cubiertos'])} - {max(estadisticas['años_cubiertos'])}")
    logger.info(f"Municipios analizados: {estadisticas['municipios_unicos']}")
    logger.info(f"Percepción promedio de seguridad: {estadisticas['promedio_pct_seguros']}%")
    logger.info(f"Percepción promedio de inseguridad: {estadisticas['promedio_pct_inseguros']}%")
    
    logger.info("\n📊 Reportes generados:")
    logger.info("  1. reports/dataset_maestro_yucatan.csv")
    logger.info("  2. reports/reporte_ensu_yucatan.html")
    logger.info("  3. reports/estadisticas_finales.json")
    
    return True

if __name__ == "__main__":
    exito = main()
    exit(0 if exito else 1)
