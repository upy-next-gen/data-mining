#!/usr/bin/env python3
"""
Fase 5: Generación de Reporte Final
Genera un reporte HTML y JSON con los resultados del procesamiento
"""

import json
import pandas as pd
import os
from pathlib import Path
import logging
from datetime import datetime

# Configurar logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/fase5_reporte.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def cargar_datos_procesados():
    """
    Carga todos los archivos procesados de Yucatán
    """
    directorio = 'data/yucatan_processed'
    datos_consolidados = []
    
    if not Path(directorio).exists():
        logger.warning(f"No existe el directorio {directorio}")
        return pd.DataFrame()
    
    archivos = [f for f in os.listdir(directorio) if f.endswith('.csv')]
    logger.info(f"Archivos procesados encontrados: {len(archivos)}")
    
    for archivo in archivos:
        ruta = os.path.join(directorio, archivo)
        try:
            df = pd.read_csv(ruta)
            logger.debug(f"Cargado: {archivo} ({len(df)} registros)")
            datos_consolidados.append(df)
        except Exception as e:
            logger.error(f"Error al cargar {archivo}: {str(e)}")
    
    if datos_consolidados:
        df_total = pd.concat(datos_consolidados, ignore_index=True)
        logger.info(f"Total de registros consolidados: {len(df_total)}")
        return df_total
    else:
        return pd.DataFrame()

def generar_estadisticas_globales(df):
    """
    Genera estadísticas globales del dataset consolidado
    """
    if df.empty:
        return {}
    
    estadisticas = {
        'total_registros': len(df),
        'trimestres_unicos': df[['AÑO', 'TRIMESTRE']].drop_duplicates().shape[0] if 'AÑO' in df.columns else 0,
        'municipios_unicos': df['NOM_MUN'].nunique() if 'NOM_MUN' in df.columns else 0,
        'ciudades_unicas': df['NOM_CD'].nunique() if 'NOM_CD' in df.columns else 0
    }
    
    # Promedios generales
    if 'PCT_SEGUROS' in df.columns:
        estadisticas['promedio_pct_seguros'] = round(df['PCT_SEGUROS'].mean(), 2)
        estadisticas['promedio_pct_inseguros'] = round(df['PCT_INSEGUROS'].mean(), 2)
        estadisticas['promedio_pct_no_responde'] = round(df['PCT_NO_RESPONDE'].mean(), 2)
    
    # Tendencia temporal
    if 'AÑO' in df.columns and 'TRIMESTRE' in df.columns:
        df['periodo'] = df['AÑO'].astype(str) + '_T' + df['TRIMESTRE'].astype(str)
        tendencia = df.groupby('periodo').agg({
            'PCT_SEGUROS': 'mean',
            'PCT_INSEGUROS': 'mean',
            'TOTAL_REGISTROS': 'sum'
        }).round(2).to_dict('index')
        estadisticas['tendencia_temporal'] = tendencia
    
    return estadisticas

def generar_html_reporte(estadisticas, resumen_procesamiento):
    """
    Genera un reporte HTML con los resultados
    """
    html = f"""
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Reporte ENSU Yucatán - {datetime.now().strftime('%Y-%m-%d')}</title>
    <style>
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 20px;
            background-color: #f5f5f5;
        }}
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            background-color: white;
            padding: 30px;
            border-radius: 10px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }}
        h1 {{
            color: #2c3e50;
            border-bottom: 3px solid #3498db;
            padding-bottom: 10px;
        }}
        h2 {{
            color: #34495e;
            margin-top: 30px;
        }}
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin: 20px 0;
        }}
        .stat-card {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 20px;
            border-radius: 8px;
            text-align: center;
        }}
        .stat-value {{
            font-size: 2em;
            font-weight: bold;
            margin: 10px 0;
        }}
        .stat-label {{
            font-size: 0.9em;
            opacity: 0.9;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
        }}
        th {{
            background-color: #3498db;
            color: white;
            padding: 12px;
            text-align: left;
        }}
        td {{
            padding: 10px;
            border-bottom: 1px solid #ddd;
        }}
        tr:hover {{
            background-color: #f5f5f5;
        }}
        .success {{
            color: #27ae60;
            font-weight: bold;
        }}
        .warning {{
            color: #f39c12;
            font-weight: bold;
        }}
        .footer {{
            margin-top: 40px;
            padding-top: 20px;
            border-top: 1px solid #ddd;
            text-align: center;
            color: #7f8c8d;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>📊 Reporte de Análisis ENSU - Yucatán</h1>
        <p><strong>Fecha de generación:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        
        <h2>📈 Estadísticas Globales</h2>
        <div class="stats-grid">
            <div class="stat-card">
                <div class="stat-label">Total Registros</div>
                <div class="stat-value">{estadisticas.get('total_registros', 0)}</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Trimestres Analizados</div>
                <div class="stat-value">{estadisticas.get('trimestres_unicos', 0)}</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Municipios</div>
                <div class="stat-value">{estadisticas.get('municipios_unicos', 0)}</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Ciudades</div>
                <div class="stat-value">{estadisticas.get('ciudades_unicas', 0)}</div>
            </div>
        </div>
        
        <h2>🎯 Promedios de Percepción</h2>
        <div class="stats-grid">
            <div class="stat-card" style="background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);">
                <div class="stat-label">% Promedio Seguros</div>
                <div class="stat-value">{estadisticas.get('promedio_pct_seguros', 0)}%</div>
            </div>
            <div class="stat-card" style="background: linear-gradient(135deg, #eb3349 0%, #f45c43 100%);">
                <div class="stat-label">% Promedio Inseguros</div>
                <div class="stat-value">{estadisticas.get('promedio_pct_inseguros', 0)}%</div>
            </div>
            <div class="stat-card" style="background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);">
                <div class="stat-label">% No Responde</div>
                <div class="stat-value">{estadisticas.get('promedio_pct_no_responde', 0)}%</div>
            </div>
        </div>
        
        <h2>📁 Resumen del Procesamiento</h2>
        <table>
            <tr>
                <th>Métrica</th>
                <th>Valor</th>
            </tr>
            <tr>
                <td>Total archivos procesados</td>
                <td>{resumen_procesamiento.get('total_archivos', 0)}</td>
            </tr>
            <tr>
                <td>Archivos exitosos</td>
                <td class="success">{resumen_procesamiento.get('archivos_exitosos', 0)}</td>
            </tr>
            <tr>
                <td>Archivos sin datos de Yucatán</td>
                <td class="warning">{resumen_procesamiento.get('archivos_sin_datos', 0)}</td>
            </tr>
            <tr>
                <td>Archivos con error</td>
                <td>{resumen_procesamiento.get('archivos_con_error', 0)}</td>
            </tr>
        </table>
"""
    
    # Agregar tendencia temporal si existe
    if 'tendencia_temporal' in estadisticas and estadisticas['tendencia_temporal']:
        html += """
        <h2>📅 Tendencia Temporal</h2>
        <table>
            <tr>
                <th>Periodo</th>
                <th>% Seguros</th>
                <th>% Inseguros</th>
                <th>Total Registros</th>
            </tr>
"""
        for periodo, valores in sorted(estadisticas['tendencia_temporal'].items()):
            html += f"""
            <tr>
                <td>{periodo}</td>
                <td>{valores.get('PCT_SEGUROS', 0):.1f}%</td>
                <td>{valores.get('PCT_INSEGUROS', 0):.1f}%</td>
                <td>{valores.get('TOTAL_REGISTROS', 0)}</td>
            </tr>
"""
        html += "</table>"
    
    html += """
        <div class="footer">
            <p>Reporte generado automáticamente por el sistema de procesamiento ENSU</p>
            <p>© 2024 - Análisis de Percepción de Seguridad en Yucatán</p>
        </div>
    </div>
</body>
</html>
"""
    
    return html

def main():
    """
    Función principal de la Fase 5
    """
    logger.info("=== FASE 5: GENERACIÓN DE REPORTE FINAL ===")
    
    # Cargar resumen de procesamiento
    resumen_path = 'temp/resumen_procesamiento.json'
    if not Path(resumen_path).exists():
        logger.error(f"No se encontró el resumen en {resumen_path}")
        logger.error("Ejecute primero la Fase 4")
        return False
    
    with open(resumen_path, 'r', encoding='utf-8') as f:
        resumen_procesamiento = json.load(f)
    
    # Cargar datos procesados
    df_consolidado = cargar_datos_procesados()
    
    # Generar estadísticas
    estadisticas = generar_estadisticas_globales(df_consolidado)
    
    # Guardar resumen ejecutivo JSON
    resumen_ejecutivo = {
        'fecha_generacion': datetime.now().isoformat(),
        'estadisticas_globales': estadisticas,
        'resumen_procesamiento': resumen_procesamiento,
        'archivos_procesados': os.listdir('data/yucatan_processed') if Path('data/yucatan_processed').exists() else []
    }
    
    os.makedirs('reports', exist_ok=True)
    
    json_path = 'reports/resumen_ejecutivo.json'
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(resumen_ejecutivo, f, indent=2, ensure_ascii=False)
    logger.info(f"Resumen ejecutivo guardado en: {json_path}")
    
    # Generar reporte HTML
    html = generar_html_reporte(estadisticas, resumen_procesamiento)
    
    html_path = f'reports/reporte_final_{datetime.now().strftime("%Y%m%d_%H%M%S")}.html'
    with open(html_path, 'w', encoding='utf-8') as f:
        f.write(html)
    logger.info(f"Reporte HTML guardado en: {html_path}")
    
    # Resumen final
    logger.info("\n=== RESUMEN FASE 5 ===")
    logger.info(f"Reporte JSON: {json_path}")
    logger.info(f"Reporte HTML: {html_path}")
    logger.info(f"Total registros analizados: {estadisticas.get('total_registros', 0)}")
    logger.info(f"Trimestres procesados: {estadisticas.get('trimestres_unicos', 0)}")
    
    if estadisticas.get('promedio_pct_seguros'):
        logger.info(f"Percepción promedio de seguridad: {estadisticas['promedio_pct_seguros']}%")
        logger.info(f"Percepción promedio de inseguridad: {estadisticas['promedio_pct_inseguros']}%")
    
    logger.info("\n✅ PIPELINE COMPLETO EJECUTADO CON ÉXITO")
    
    return True

if __name__ == "__main__":
    exito = main()
    exit(0 if exito else 1)