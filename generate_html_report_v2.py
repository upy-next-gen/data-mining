#!/usr/bin/env python3
"""
Generador de Reporte HTML Interactivo - Versión 2 Simplificada
Análisis de Percepción de Seguridad Urbana - Yucatán 2016-2025
"""

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score
import base64
import os
from datetime import datetime
import json
import warnings
warnings.filterwarnings('ignore')

# Configuración de colores UPY y complementarios
COLORS = {
    'upy_orange': '#FB9D09',
    'upy_purple': '#5C0C8A', 
    'merida': '#2E8B57',      
    'kanasin': '#4682B4',     
    'progreso': '#CD853F',    
    'uman': '#8B4513',        
    'background': '#FAFAFA',
    'text': '#2C3E50',
    'grid': '#E8E8E8',
    'confidence': 'rgba(200, 200, 200, 0.3)'
}

def load_and_prepare_data(file_path):
    """Carga y prepara los datos unificados"""
    print("📊 Cargando datos...")
    df = pd.read_csv(file_path)
    
    # Unificar nombres de municipios
    municipality_mapping = {
        'MERIDA': 'Mérida', 'Merida': 'Mérida',
        'KANASIN': 'Kanasín', 'Kanasin': 'Kanasín', 
        'PROGRESO': 'Progreso', 'Progreso': 'Progreso',
        'UMAN': 'Umán', 'Uman': 'Umán'
    }
    
    df['NOM_MUN'] = df['NOM_MUN'].map(municipality_mapping)
    df['PERIODO'] = df['ANO'] + (df['TRIMESTRE'] - 1) * 0.25
    df['PERIODO_STR'] = df['ANO'].astype(str) + 'Q' + df['TRIMESTRE'].astype(str)
    df = df.sort_values(['PERIODO', 'NOM_MUN']).reset_index(drop=True)
    
    print(f"✅ Datos cargados: {len(df)} registros")
    return df

def calculate_regression_and_prediction(df_mun, periods_ahead=4):
    """Calcula regresión lineal y predicción"""
    if len(df_mun) < 3:
        return None, None, None, None, None
    
    X = df_mun['PERIODO'].values.reshape(-1, 1)
    y = df_mun['PCT_INSEGUROS'].values
    
    model = LinearRegression()
    model.fit(X, y)
    
    y_pred = model.predict(X)
    r2 = r2_score(y, y_pred)
    
    # Predicción futura
    last_period = df_mun['PERIODO'].max()
    future_periods = np.arange(last_period + 0.25, last_period + 0.25 * (periods_ahead + 1), 0.25)
    future_y = model.predict(future_periods.reshape(-1, 1))
    
    # Intervalo de confianza
    residuals = y - y_pred
    mse = np.mean(residuals ** 2)
    confidence_interval = 1.96 * np.sqrt(mse)
    
    return model, r2, future_periods, future_y, confidence_interval

def create_municipality_data(df):
    """Prepara todos los datos por municipio"""
    municipalities = ['Mérida', 'Kanasín', 'Progreso', 'Umán']
    municipality_data = {}
    
    for municipality in municipalities:
        print(f"📈 Preparando datos para {municipality}")
        df_mun = df[df['NOM_MUN'] == municipality].sort_values('PERIODO')
        
        if len(df_mun) == 0:
            continue
            
        # Calcular regresión
        model, r2, future_periods, future_y, conf_interval = calculate_regression_and_prediction(df_mun)
        
        # Preparar datos de predicción
        future_quarters = []
        if future_periods is not None:
            for period in future_periods:
                year = int(period)
                quarter = int((period - year) * 4) + 1
                future_quarters.append(f"{year}Q{quarter}")
        
        municipality_data[municipality] = {
            'historical': {
                'x': df_mun['PERIODO_STR'].tolist(),
                'y': [float(x) for x in df_mun['PCT_INSEGUROS'].tolist()],
                'customdata': [[int(x[0]), int(x[1]), int(x[2])] for x in df_mun[['TOTAL_REGISTROS', 'TOTAL_SEGUROS', 'TOTAL_INSEGUROS']].values.tolist()]
            },
            'regression': {
                'x': df_mun['PERIODO_STR'].tolist(),
                'y': [float(x) for x in model.predict(df_mun['PERIODO'].values.reshape(-1, 1)).tolist()] if model else [],
                'r2': float(r2) if r2 else 0.0
            },
            'prediction': {
                'x': future_quarters,
                'y': [float(x) for x in future_y.tolist()] if future_y is not None else [],
                'upper': [float(x) for x in (future_y + conf_interval).tolist()] if future_y is not None and conf_interval is not None else [],
                'lower': [float(x) for x in (future_y - conf_interval).tolist()] if future_y is not None and conf_interval is not None else []
            },
            'stats': {
                'avg_insecurity': float(df_mun['PCT_INSEGUROS'].mean()),
                'total_surveys': int(df_mun['TOTAL_REGISTROS'].sum()),
                'min_period': str(df_mun.loc[df_mun['PCT_INSEGUROS'].idxmin(), 'PERIODO_STR']),
                'max_period': str(df_mun.loc[df_mun['PCT_INSEGUROS'].idxmax(), 'PERIODO_STR']),
                'min_value': float(df_mun['PCT_INSEGUROS'].min()),
                'max_value': float(df_mun['PCT_INSEGUROS'].max()),
                'trend': 'mejorando' if model and model.coef_[0] < 0 else 'empeorando' if model and model.coef_[0] > 0 else 'estable'
            }
        }
        
        r2_value = r2 if r2 is not None else 0
        print(f"   ✅ {municipality}: {len(df_mun)} registros, R²={r2_value:.3f}")
    
    return municipality_data

def encode_image_to_base64(image_path):
    """Convierte imagen a base64"""
    try:
        with open(image_path, "rb") as image_file:
            encoded_string = base64.b64encode(image_file.read()).decode('utf-8')
            return f"data:image/png;base64,{encoded_string}"
    except FileNotFoundError:
        print(f"⚠️  Logo no encontrado en {image_path}")
        return ""

def generate_html_report(municipality_data, logo_base64):
    """Genera el reporte HTML completo"""
    print("🌐 Generando reporte HTML...")
    
    # Convertir datos a JSON para JavaScript
    data_json = json.dumps(municipality_data, indent=2)
    
    # Generar tarjetas de estadísticas
    stats_cards = ""
    municipalities = ['Mérida', 'Kanasín', 'Progreso', 'Umán']
    
    for i, municipality in enumerate(municipalities):
        if municipality not in municipality_data:
            continue
            
        data = municipality_data[municipality]
        municipality_key = municipality.lower().replace('á', 'a').replace('í', 'i').replace('é', 'e')
        
        print(f"🔍 DEBUGGING ID: {municipality} → {municipality_key}")
        
        # Color del municipio
        color = COLORS.get(municipality_key, COLORS['upy_purple'])
        
        # Advertencia de datos limitados
        limited_warning = ""
        if data['stats']['total_surveys'] < 500:
            limited_warning = '<div class="limited-data-warning">⚠️ Datos limitados</div>'
        
        trend_icon = "📈" if data['stats']['trend'] == 'empeorando' else "📉" if data['stats']['trend'] == 'mejorando' else "➡️"
        
        stats_cards += f'''
        <div class="stats-card" id="stats-{municipality_key}" style="display: {'block' if i == 0 else 'none'};">
            <div class="card-header" style="border-left: 4px solid {color};">
                <h3>{municipality}</h3>
                {limited_warning}
            </div>
            <div class="stats-grid">
                <div class="stat-item">
                    <div class="stat-value">{data['stats']['avg_insecurity']:.1f}%</div>
                    <div class="stat-label">Promedio Inseguridad</div>
                </div>
                <div class="stat-item">
                    <div class="stat-value">{trend_icon} {data['stats']['trend'].title()}</div>
                    <div class="stat-label">Tendencia (R² = {data['regression']['r2']:.3f})</div>
                </div>
                <div class="stat-item">
                    <div class="stat-value">{data['stats']['total_surveys']:,}</div>
                    <div class="stat-label">Total Encuestas</div>
                </div>
                <div class="stat-item">
                    <div class="stat-value">{data['stats']['min_value']:.1f}% ({data['stats']['min_period']})</div>
                    <div class="stat-label">Menor Inseguridad</div>
                </div>
                <div class="stat-item">
                    <div class="stat-value">{data['stats']['max_value']:.1f}% ({data['stats']['max_period']})</div>
                    <div class="stat-label">Mayor Inseguridad</div>
                </div>
                <div class="stat-item">
                    <div class="stat-value">{len(data['historical']['x'])}</div>
                    <div class="stat-label">Períodos Registrados</div>
                </div>
            </div>
        </div>
        '''
    
    # HTML completo
    html_content = f'''
    <!DOCTYPE html>
    <html lang="es">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Análisis de Percepción de Seguridad Urbana - Yucatán 2016-2025</title>
        <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
        <style>
            * {{ margin: 0; padding: 0; box-sizing: border-box; }}
            
            body {{
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                line-height: 1.6;
                color: {COLORS['text']};
                background-color: {COLORS['background']};
            }}
            
            .container {{ max-width: 1200px; margin: 0 auto; padding: 20px; }}
            
            .header {{
                background: linear-gradient(135deg, {COLORS['upy_purple']}, {COLORS['upy_orange']});
                color: white; padding: 30px 0; text-align: center; margin-bottom: 30px;
                border-radius: 10px; box-shadow: 0 4px 8px rgba(0,0,0,0.1);
            }}
            
            .header-content {{ display: flex; align-items: center; justify-content: center; gap: 20px; flex-wrap: wrap; }}
            .logo {{ height: 60px; width: auto; }}
            .header h1 {{ font-size: 2.2em; font-weight: 300; margin-bottom: 10px; }}
            .header p {{ font-size: 1.1em; opacity: 0.9; }}
            
            .tabs-container {{ margin: 30px 0; text-align: center; }}
            .tabs {{ display: inline-flex; background: white; border-radius: 25px; padding: 5px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); gap: 5px; }}
            
            .tab-button {{
                padding: 12px 24px; border: none; background: transparent; color: {COLORS['text']};
                cursor: pointer; border-radius: 20px; font-weight: 500; font-size: 16px;
                transition: all 0.3s ease; position: relative; overflow: hidden;
            }}
            
            .tab-button:hover {{ transform: translateY(-2px); box-shadow: 0 4px 8px rgba(0,0,0,0.1); }}
            .tab-button.active {{ color: white; font-weight: 600; transform: translateY(-2px); box-shadow: 0 4px 12px rgba(0,0,0,0.2); }}
            
            .tab-button.merida.active {{ background: linear-gradient(135deg, #2E8B57, #3CB371); }}
            .tab-button.kanasin.active {{ background: linear-gradient(135deg, #4682B4, #87CEEB); }}
            .tab-button.progreso.active {{ background: linear-gradient(135deg, #CD853F, #DEB887); }}
            .tab-button.uman.active {{ background: linear-gradient(135deg, #8B4513, #D2691E); }}
            
            .plot-container {{ background: white; border-radius: 10px; padding: 20px; margin: 20px 0; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
            
            .stats-section {{ margin: 30px 0; }}
            .stats-card {{ background: white; border-radius: 10px; padding: 25px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); margin-bottom: 20px; }}
            .card-header {{ display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; padding-left: 15px; }}
            .card-header h3 {{ color: {COLORS['text']}; font-size: 1.4em; font-weight: 600; }}
            .limited-data-warning {{ background: #FFF3CD; color: #856404; padding: 5px 10px; border-radius: 15px; font-size: 0.9em; font-weight: 500; }}
            .stats-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; }}
            .stat-item {{ text-align: center; padding: 15px; background: #F8F9FA; border-radius: 8px; transition: transform 0.2s ease; }}
            .stat-item:hover {{ transform: translateY(-2px); }}
            .stat-value {{ font-size: 1.5em; font-weight: 700; color: {COLORS['upy_purple']}; margin-bottom: 5px; }}
            .stat-label {{ color: #6C757D; font-size: 0.9em; font-weight: 500; }}
            
            .storytelling-section {{
                background: white; border-radius: 10px; padding: 30px; margin: 30px 0;
                box-shadow: 0 2px 10px rgba(0,0,0,0.1); line-height: 1.8;
            }}
            .storytelling-section h2 {{ color: {COLORS['upy_purple']}; font-size: 1.8em; margin-bottom: 25px; border-bottom: 3px solid {COLORS['upy_orange']}; padding-bottom: 10px; }}
            .analysis-paragraph {{ margin-bottom: 20px; }}
            .analysis-paragraph p {{ text-align: justify; margin-bottom: 15px; }}
            
            .methodology-note {{
                background: #F8F9FA; border-left: 4px solid {COLORS['upy_orange']}; padding: 20px; margin-top: 30px; border-radius: 0 8px 8px 0;
            }}
            .methodology-note h3 {{ color: {COLORS['upy_purple']}; margin-bottom: 15px; }}
            .methodology-note ul {{ padding-left: 20px; }}
            .methodology-note li {{ margin-bottom: 8px; }}
            
            .footer {{ background: {COLORS['text']}; color: white; text-align: center; padding: 20px; margin-top: 40px; border-radius: 10px; }}
            .footer p {{ margin-bottom: 5px; }}
            
            .export-button {{
                background: {COLORS['upy_orange']}; color: white; border: none; padding: 10px 20px;
                border-radius: 5px; cursor: pointer; font-weight: 500; margin: 10px 5px;
                transition: background 0.3s ease;
            }}
            .export-button:hover {{ background: #E6890A; transform: translateY(-1px); }}
        </style>
    </head>
    <body>
        <div class="container">
            <!-- Header -->
            <div class="header">
                <div class="header-content">
                    {"<img src='" + logo_base64 + "' alt='Logo UPY' class='logo'>" if logo_base64 else ""}
                    <div>
                        <h1>Análisis de Percepción de Seguridad Urbana</h1>
                        <p>Yucatán 2016-2025 | Datos ENSU-INEGI</p>
                    </div>
                </div>
            </div>
            
            <!-- Pestañas -->
            <div class="tabs-container">
                <div class="tabs">
                    <button class="tab-button merida active" onclick="switchMunicipality('merida')">Mérida</button>
                    <button class="tab-button kanasin" onclick="switchMunicipality('kanasin')">Kanasín</button>
                    <button class="tab-button progreso" onclick="switchMunicipality('progreso')">Progreso</button>
                    <button class="tab-button uman" onclick="switchMunicipality('uman')">Umán</button>
                </div>
            </div>
            
            <!-- Gráfica Principal -->
            <div class="plot-container">
                <div id="main-plot" style="width:100%;height:500px;"></div>
            </div>
            
            <!-- Estadísticas por Municipio -->
            <div class="stats-section">
                {stats_cards}
            </div>
            
            <!-- Storytelling -->
            <div class="storytelling-section">
                <h2>📊 Análisis de la Evolución de la Percepción de Inseguridad en Yucatán</h2>
                
                <div class="analysis-paragraph">
                    <p><strong>Panorama General:</strong> El análisis revela patrones diferenciados entre municipios yucatecos durante 2016-2025, con Mérida mostrando menores niveles de inseguridad promedio (25.3%) comparado con Kanasín (51.0%), Progreso (42.6%) y Umán (42.3%).</p>
                </div>
                
                <div class="analysis-paragraph">
                    <p><strong>Tendencias Identificadas:</strong> Kanasín presenta la mayor volatilidad y percepción de inseguridad, mientras que Mérida mantiene niveles más estables. Las proyecciones sugieren continuidad en estas tendencias diferenciadas entre municipios.</p>
                </div>
                
                <div class="methodology-note">
                    <h3>⚠️ Consideraciones Metodológicas</h3>
                    <ul>
                        <li>Los municipios distintos a Mérida cuentan con muestras más pequeñas, incrementando el margen de error.</li>
                        <li>Las predicciones asumen continuidad de tendencias actuales.</li>
                        <li>Los intervalos de confianza reflejan la variabilidad histórica de cada municipio.</li>
                    </ul>
                </div>
            </div>
            
            <!-- Footer -->
            <div class="footer">
                <p><strong>Fuente:</strong> Instituto Nacional de Estadística y Geografía (INEGI) - Encuesta Nacional de Seguridad Urbana (ENSU)</p>
                <p><strong>Procesamiento:</strong> Universidad Politécnica de Yucatán | Generado el {datetime.now().strftime('%d de %B de %Y')}</p>
                <button class="export-button" onclick="exportChart()">📊 Exportar Gráfica</button>
            </div>
        </div>
        
        <script>
            // Datos completos embebidos
            const municipalityData = {data_json};
            
            // Configuración de colores
            const colors = {{
                'merida': '#2E8B57',
                'kanasin': '#4682B4', 
                'progreso': '#CD853F',
                'uman': '#8B4513'
            }};
            
            let currentPlot = null;
            
            function createPlot(municipalityKey) {{
                console.log('🎯 Creando gráfica para:', municipalityKey);
                
                const municipalityNames = {{
                    'merida': 'Mérida',
                    'kanasin': 'Kanasín',
                    'progreso': 'Progreso', 
                    'uman': 'Umán'
                }};
                
                const municipalityName = municipalityNames[municipalityKey];
                const data = municipalityData[municipalityName];
                
                if (!data) {{
                    console.error('❌ No hay datos para:', municipalityName);
                    return;
                }}
                
                console.log('📊 Datos encontrados:', data);
                
                const traces = [];
                const color = colors[municipalityKey];
                
                // Traza histórica
                traces.push({{
                    x: data.historical.x,
                    y: data.historical.y,
                    mode: 'lines+markers',
                    name: municipalityName + ' (Histórico)',
                    line: {{ color: color, width: 3 }},
                    marker: {{ size: 8, color: color }},
                    customdata: data.historical.customdata,
                    hovertemplate: '<b>' + municipalityName + '</b><br>' +
                                  'Período: %{{x}}<br>' +
                                  'Inseguridad: %{{y:.1f}}%<br>' +
                                  'Encuestas: %{{customdata[0]}}<br>' +
                                  'Seguros: %{{customdata[1]}}<br>' +
                                  'Inseguros: %{{customdata[2]}}<br>' +
                                  '<extra></extra>'
                }});
                
                // Traza de regresión
                if (data.regression.y.length > 0) {{
                    traces.push({{
                        x: data.regression.x,
                        y: data.regression.y,
                        mode: 'lines',
                        name: municipalityName + ' (Regresión)',
                        line: {{ color: color, width: 2, dash: 'dash' }},
                        hovertemplate: '<b>Regresión ' + municipalityName + '</b><br>' +
                                      'R² = ' + data.regression.r2.toFixed(3) + '<br>' +
                                      '<extra></extra>'
                    }});
                }}
                
                // Traza de predicción
                if (data.prediction.y.length > 0) {{
                    traces.push({{
                        x: data.prediction.x,
                        y: data.prediction.y,
                        mode: 'lines+markers',
                        name: municipalityName + ' (Predicción)',
                        line: {{ color: color, width: 2, dash: 'dot' }},
                        marker: {{ size: 6, color: color, symbol: 'diamond' }},
                        hovertemplate: '<b>Predicción ' + municipalityName + '</b><br>' +
                                      'Período: %{{x}}<br>' +
                                      'Inseguridad proyectada: %{{y:.1f}}%<br>' +
                                      '<extra></extra>'
                    }});
                    
                    // Intervalo de confianza
                    if (data.prediction.upper.length > 0) {{
                        traces.push({{
                            x: data.prediction.x.concat(data.prediction.x.slice().reverse()),
                            y: data.prediction.upper.concat(data.prediction.lower.slice().reverse()),
                            fill: 'toself',
                            fillcolor: 'rgba(200, 200, 200, 0.3)',
                            line: {{ color: 'rgba(255,255,255,0)' }},
                            hoverinfo: 'skip',
                            showlegend: false,
                            name: municipalityName + ' (IC 95%)'
                        }});
                    }}
                }}
                
                const layout = {{
                    title: {{
                        text: 'Evolución de la Percepción de Inseguridad - ' + municipalityName,
                        x: 0.5,
                        xanchor: 'center',
                        font: {{ size: 20, color: '#2C3E50' }}
                    }},
                    xaxis: {{
                        title: 'Período',
                        showgrid: true,
                        gridwidth: 1,
                        gridcolor: '#E8E8E8',
                        tickangle: 45
                    }},
                    yaxis: {{
                        title: 'Porcentaje de Percepción de Inseguridad (%)',
                        showgrid: true,
                        gridwidth: 1,
                        gridcolor: '#E8E8E8',
                        range: [0, 100]
                    }},
                    font: {{ family: "Arial", size: 12, color: '#2C3E50' }},
                    plot_bgcolor: 'white',
                    paper_bgcolor: 'white',
                    showlegend: true,
                    legend: {{
                        orientation: "h",
                        yanchor: "bottom",
                        y: 1.02,
                        xanchor: "right",
                        x: 1
                    }},
                    hovermode: 'closest',
                    height: 500
                }};
                
                const config = {{
                    displayModeBar: true,
                    toImageButtonOptions: {{
                        format: 'png',
                        filename: 'percepcion_inseguridad_' + municipalityKey,
                        height: 500,
                        width: 1000,
                        scale: 1
                    }}
                }};
                
                Plotly.newPlot('main-plot', traces, layout, config);
                console.log('✅ Gráfica creada exitosamente para', municipalityName);
            }}
            
            function switchMunicipality(municipalityKey) {{
                console.log('🔄 Cambiando a:', municipalityKey);
                
                // Debugging específico para Mérida
                if (municipalityKey === 'merida') {{
                    console.log('🔍 DEBUGGING MÉRIDA - Iniciando análisis específico');
                }}
                
                // Actualizar pestañas
                document.querySelectorAll('.tab-button').forEach(tab => tab.classList.remove('active'));
                const activeTab = document.querySelector('.tab-button.' + municipalityKey);
                if (activeTab) {{
                    activeTab.classList.add('active');
                    console.log('✅ Pestaña activada:', activeTab.textContent);
                }}
                
                // Actualizar gráfica
                createPlot(municipalityKey);
                
                // Debugging ultra-específico para tarjetas
                console.log('🔍 Buscando tarjeta con ID: stats-' + municipalityKey);
                
                // Actualizar tarjeta de estadísticas con debugging
                document.querySelectorAll('.stats-card').forEach(card => {{
                    card.style.display = 'none';
                    console.log('🔄 Ocultando tarjeta:', card.id);
                }});
                
                const statsCard = document.getElementById('stats-' + municipalityKey);
                console.log('🔍 Elemento encontrado:', statsCard);
                console.log('🔍 ID buscado:', 'stats-' + municipalityKey);
                
                if (statsCard) {{
                    statsCard.style.display = 'block';
                    console.log('✅ Tarjeta mostrada:', statsCard.id);
                }} else {{
                    console.error('❌ Tarjeta NO encontrada para:', municipalityKey);
                    console.log('🔍 Tarjetas disponibles en DOM:');
                    document.querySelectorAll('.stats-card').forEach(card => {{
                        console.log('   - ID disponible:', card.id);
                    }});
                }}
            }}
            
            function exportChart() {{
                Plotly.toImage('main-plot', {{
                    format: 'png',
                    width: 1000,
                    height: 600,
                    scale: 2
                }}).then(function(url) {{
                    const link = document.createElement('a');
                    link.download = 'percepcion_inseguridad_yucatan.png';
                    link.href = url;
                    link.click();
                }});
            }}
            
            // Inicialización
            document.addEventListener('DOMContentLoaded', function() {{
                console.log('🚀 Inicializando aplicación...');
                console.log('📊 Datos disponibles:', Object.keys(municipalityData));
                
                // Pequeño delay para asegurar que Plotly esté listo
                setTimeout(() => {{
                    switchMunicipality('merida');
                }}, 500);
            }});
        </script>
    </body>
    </html>
    '''
    
    return html_content

def main():
    """Función principal"""
    print("🚀 Iniciando generación de reporte HTML v2...")
    
    # Rutas
    data_path = "data/yucatan-inseguridad/data-yucatan-inseguridad.csv"
    logo_path = "resources/logo-upy.png"
    output_path = "reporte_analisis_yucatan.html"
    
    try:
        # Cargar datos
        df = load_and_prepare_data(data_path)
        
        # Preparar datos por municipio
        municipality_data = create_municipality_data(df)
        
        # Codificar logo
        logo_base64 = encode_image_to_base64(logo_path)
        
        # Generar HTML
        html_content = generate_html_report(municipality_data, logo_base64)
        
        # Guardar
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        print(f"✅ Reporte v2 generado: {output_path}")
        print("🌐 Abrir en navegador para probar")
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        raise

if __name__ == "__main__":
    main()
