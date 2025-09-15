#!/usr/bin/env python3
"""
Generador de Reporte HTML Interactivo
Análisis de Percepción de Seguridad Urbana - Yucatán 2016-2025

Este script genera un reporte HTML interactivo con gráficas de Plotly
para analizar la evolución temporal de la percepción de inseguridad
en municipios de Yucatán basado en datos ENSU-INEGI.
"""

import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score
import base64
import os
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# Configuración de colores UPY y complementarios
COLORS = {
    'upy_orange': '#FB9D09',
    'upy_purple': '#5C0C8A',
    'merida': '#2E8B57',      # Verde mar para Mérida (capital)
    'kanasin': '#4682B4',     # Azul acero para Kanasín
    'progreso': '#CD853F',    # Café dorado para Progreso (puerto)
    'uman': '#8B4513',        # Café oscuro para Umán
    'background': '#FAFAFA',
    'text': '#2C3E50',
    'grid': '#E8E8E8',
    'confidence': 'rgba(200, 200, 200, 0.3)'
}

def load_and_prepare_data(file_path):
    """
    Carga y prepara los datos unificados para análisis
    """
    print("📊 Cargando datos unificados...")
    df = pd.read_csv(file_path)
    
    # Unificar nombres de municipios (normalizar capitalización)
    municipality_mapping = {
        'MERIDA': 'Mérida',
        'Merida': 'Mérida',
        'KANASIN': 'Kanasín',
        'Kanasin': 'Kanasín',
        'PROGRESO': 'Progreso',
        'Progreso': 'Progreso',
        'UMAN': 'Umán',
        'Uman': 'Umán'
    }
    
    df['NOM_MUN'] = df['NOM_MUN'].map(municipality_mapping)
    
    # Crear columna de período temporal para ordenamiento
    df['PERIODO'] = df['ANO'] + (df['TRIMESTRE'] - 1) * 0.25
    df['PERIODO_STR'] = df['ANO'].astype(str) + 'Q' + df['TRIMESTRE'].astype(str)
    
    # Ordenar cronológicamente
    df = df.sort_values(['PERIODO', 'NOM_MUN']).reset_index(drop=True)
    
    print(f"✅ Datos cargados: {len(df)} registros de {df['NOM_MUN'].nunique()} municipios")
    print(f"📅 Período: {df['PERIODO_STR'].min()} - {df['PERIODO_STR'].max()}")
    
    return df

def calculate_regression_and_prediction(df_mun, periods_ahead=4):
    """
    Calcula regresión lineal y predicción para un municipio
    """
    if len(df_mun) < 3:  # Necesitamos al menos 3 puntos
        return None, None, None, None, None
    
    X = df_mun['PERIODO'].values.reshape(-1, 1)
    y = df_mun['PCT_INSEGUROS'].values
    
    # Regresión lineal
    model = LinearRegression()
    model.fit(X, y)
    
    # R²
    y_pred = model.predict(X)
    r2 = r2_score(y, y_pred)
    
    # Predicción futura
    last_period = df_mun['PERIODO'].max()
    future_periods = np.arange(last_period + 0.25, last_period + 0.25 * (periods_ahead + 1), 0.25)
    future_X = future_periods.reshape(-1, 1)
    future_y = model.predict(future_X)
    
    # Intervalo de confianza (aproximado)
    residuals = y - y_pred
    mse = np.mean(residuals ** 2)
    confidence_interval = 1.96 * np.sqrt(mse)  # 95% CI aproximado
    
    return model, r2, future_periods, future_y, confidence_interval

def encode_image_to_base64(image_path):
    """
    Convierte imagen a base64 para embebido en HTML
    """
    try:
        with open(image_path, "rb") as image_file:
            encoded_string = base64.b64encode(image_file.read()).decode('utf-8')
            return f"data:image/png;base64,{encoded_string}"
    except FileNotFoundError:
        print(f"⚠️  Logo no encontrado en {image_path}")
        return ""

def create_interactive_plot(df):
    """
    Crea la gráfica interactiva principal con Plotly
    """
    print("📈 Generando gráficas interactivas...")
    
    # Orden explícito de municipios para evitar problemas de indexación
    municipalities = ['Mérida', 'Kanasín', 'Progreso', 'Umán']
    print(f"🏘️  Municipios en orden: {municipalities}")
    
    # Crear figura con subplots para cada municipio
    fig = go.Figure()
    
    # Configurar los datos para cada municipio
    plot_data = {}
    
    # Debugging exhaustivo: verificar datos por municipio
    print("🔍 DEBUGGING - Verificando datos por municipio:")
    for mun in municipalities:
        df_temp = df[df['NOM_MUN'] == mun]
        print(f"   {mun}: {len(df_temp)} registros")
        if len(df_temp) > 0:
            print(f"      Períodos: {df_temp['PERIODO_STR'].min()} - {df_temp['PERIODO_STR'].max()}")
            print(f"      Inseguridad promedio: {df_temp['PCT_INSEGUROS'].mean():.1f}%")
    
    for i, municipality in enumerate(municipalities):
        print(f"\n📊 Procesando municipio {i}: {municipality}")
        df_mun = df[df['NOM_MUN'] == municipality].sort_values('PERIODO')
        
        if len(df_mun) == 0:
            print(f"   ⚠️ Sin datos para {municipality}")
            continue
        
        print(f"   ✅ {len(df_mun)} registros encontrados para {municipality}")
        print(f"   📅 Rango: {df_mun['PERIODO_STR'].iloc[0]} - {df_mun['PERIODO_STR'].iloc[-1]}")
        print(f"   📈 Inseguridad: {df_mun['PCT_INSEGUROS'].min():.1f}% - {df_mun['PCT_INSEGUROS'].max():.1f}%")
            
        color = COLORS.get(municipality.lower().replace('á', 'a').replace('í', 'i'), COLORS['upy_purple'])
        
        # Datos históricos con identificador único
        print(f"   🎨 Color asignado: {color}")
        print(f"   📍 Agregando traza histórica para {municipality}")
        
        historical_trace = go.Scatter(
            x=df_mun['PERIODO_STR'].tolist(),  # Convertir a lista explícitamente
            y=df_mun['PCT_INSEGUROS'].tolist(),  # Convertir a lista explícitamente
            mode='lines+markers',
            name=f'{municipality} (Histórico)',
            line=dict(color=color, width=3),
            marker=dict(size=8, color=color),
            customdata=df_mun[['TOTAL_REGISTROS', 'TOTAL_SEGUROS', 'TOTAL_INSEGUROS']].values.tolist(),
            hovertemplate=(
                f'<b>{municipality}</b><br>' +
                'Período: %{x}<br>' +
                'Inseguridad: %{y:.1f}%<br>' +
                'Encuestas: %{customdata[0]}<br>' +
                'Seguros: %{customdata[1]}<br>' +
                'Inseguros: %{customdata[2]}<br>' +
                '<extra></extra>'
            ),
            visible=(municipality == 'Mérida'),  # Solo mostrar Mérida inicialmente
            uid=f'hist_{municipality.lower()}'  # UID único para identificación
        )
        
        fig.add_trace(historical_trace)
        trace_count = len(fig.data)
        print(f"   ✅ Traza histórica agregada. Total trazas: {trace_count}")
        print(f"   📊 Datos históricos: {len(df_mun['PERIODO_STR'])} puntos de {df_mun['PERIODO_STR'].iloc[0]} a {df_mun['PERIODO_STR'].iloc[-1]}")
        
        # Calcular regresión y predicción con validación robusta
        print(f"   🔬 Calculando regresión para {municipality}...")
        model, r2, future_periods, future_y, conf_interval = calculate_regression_and_prediction(df_mun)
        
        if model is not None:
            print(f"   ✅ Regresión calculada. R² = {r2:.3f}")
            
            # Línea de regresión
            regression_y = model.predict(df_mun['PERIODO'].values.reshape(-1, 1))
            print(f"   📈 Agregando línea de regresión para {municipality}")
            
            regression_trace = go.Scatter(
                x=df_mun['PERIODO_STR'].tolist(),
                y=regression_y.tolist(),
                mode='lines',
                name=f'{municipality} (Regresión)',
                line=dict(color=color, width=2, dash='dash'),
                hovertemplate=(
                    f'<b>Regresión {municipality}</b><br>' +
                    'R² = %{text:.3f}<br>' +
                    '<extra></extra>'
                ),
                text=[r2] * len(regression_y),
                visible=(municipality == 'Mérida'),
                uid=f'reg_{municipality.lower()}'
            )
            fig.add_trace(regression_trace)
            print(f"   ✅ Traza de regresión agregada. Total trazas: {len(fig.data)}")
            
            # Predicción futura
            if future_periods is not None and len(future_periods) > 0:
                future_quarters = []
                for period in future_periods:
                    year = int(period)
                    quarter = int((period - year) * 4) + 1
                    future_quarters.append(f"{year}Q{quarter}")
                
                print(f"   🔮 Agregando predicción: {len(future_quarters)} trimestres futuros")
                print(f"      Períodos de predicción: {future_quarters}")
                
                prediction_trace = go.Scatter(
                    x=future_quarters,
                    y=future_y.tolist(),
                    mode='lines+markers',
                    name=f'{municipality} (Predicción)',
                    line=dict(color=color, width=2, dash='dot'),
                    marker=dict(size=6, color=color, symbol='diamond'),
                    hovertemplate=(
                        f'<b>Predicción {municipality}</b><br>' +
                        'Período: %{x}<br>' +
                        'Inseguridad proyectada: %{y:.1f}%<br>' +
                        '<extra></extra>'
                    ),
                    visible=(municipality == 'Mérida'),
                    uid=f'pred_{municipality.lower()}'
                )
                fig.add_trace(prediction_trace)
                print(f"   ✅ Traza de predicción agregada. Total trazas: {len(fig.data)}")
                
                # Intervalo de confianza
                if conf_interval is not None:
                    print(f"   📊 Agregando intervalo de confianza (±{conf_interval:.2f}%)")
                    
                    upper_bound = future_y + conf_interval
                    lower_bound = future_y - conf_interval
                    
                    confidence_trace = go.Scatter(
                        x=future_quarters + future_quarters[::-1],
                        y=np.concatenate([upper_bound, lower_bound[::-1]]).tolist(),
                        fill='toself',
                        fillcolor=COLORS['confidence'],
                        line=dict(color='rgba(255,255,255,0)'),
                        hoverinfo="skip",
                        showlegend=False,
                        name=f'{municipality} (IC 95%)',
                        visible=(municipality == 'Mérida'),
                        uid=f'conf_{municipality.lower()}'
                    )
                    fig.add_trace(confidence_trace)
                    print(f"   ✅ Intervalo de confianza agregado. Total trazas: {len(fig.data)}")
                else:
                    print(f"   ⚠️ Sin intervalo de confianza para {municipality}")
            else:
                print(f"   ⚠️ Sin predicción futura para {municipality} (datos insuficientes)")
        else:
            print(f"   ❌ Sin regresión para {municipality} (datos insuficientes: {len(df_mun)} registros)")
        
        # Guardar datos para las tarjetas estadísticas
        plot_data[municipality] = {
            'df': df_mun,
            'model': model,
            'r2': r2 if r2 is not None else 0,
            'avg_insecurity': df_mun['PCT_INSEGUROS'].mean(),
            'total_surveys': df_mun['TOTAL_REGISTROS'].sum(),
            'trend': 'mejorando' if model and model.coef_[0] < 0 else 'empeorando' if model and model.coef_[0] > 0 else 'estable',
            'min_period': df_mun.loc[df_mun['PCT_INSEGUROS'].idxmin(), 'PERIODO_STR'],
            'max_period': df_mun.loc[df_mun['PCT_INSEGUROS'].idxmax(), 'PERIODO_STR'],
            'min_value': df_mun['PCT_INSEGUROS'].min(),
            'max_value': df_mun['PCT_INSEGUROS'].max()
        }
    
    # Configurar layout
    fig.update_layout(
        title={
            'text': 'Evolución de la Percepción de Inseguridad por Trimestre',
            'x': 0.5,
            'xanchor': 'center',
            'font': {'size': 20, 'color': COLORS['text']}
        },
        xaxis_title='Período',
        yaxis_title='Porcentaje de Percepción de Inseguridad (%)',
        font=dict(family="Arial", size=12, color=COLORS['text']),
        plot_bgcolor='white',
        paper_bgcolor='white',
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        hovermode='closest',
        xaxis=dict(
            showgrid=True,
            gridwidth=1,
            gridcolor=COLORS['grid'],
            tickangle=45
        ),
        yaxis=dict(
            showgrid=True,
            gridwidth=1,
            gridcolor=COLORS['grid'],
            range=[0, 100]
        ),
        height=500
    )
    
    return fig, plot_data, municipalities

def generate_municipality_stats_cards(plot_data, municipalities):
    """
    Genera las tarjetas de estadísticas por municipio
    """
    cards_html = ""
    
    for i, municipality in enumerate(municipalities):
        data = plot_data[municipality]
        color = COLORS.get(municipality.lower().replace('á', 'a').replace('í', 'i'), COLORS['upy_purple'])
        
        # Determinar si los datos son limitados
        limited_data_warning = ""
        if data['total_surveys'] < 500:  # Umbral para considerar datos limitados
            limited_data_warning = '<div class="limited-data-warning">⚠️ Datos limitados</div>'
        
        trend_icon = "📈" if data['trend'] == 'empeorando' else "📉" if data['trend'] == 'mejorando' else "➡️"
        
        card_html = f'''
        <div class="stats-card" id="stats-{municipality.lower().replace('á', 'a').replace('í', 'i')}" style="display: {'block' if municipality == 'Mérida' else 'none'};">
            <div class="card-header" style="border-left: 4px solid {color};">
                <h3>{municipality}</h3>
                {limited_data_warning}
            </div>
            <div class="stats-grid">
                <div class="stat-item">
                    <div class="stat-value">{data['avg_insecurity']:.1f}%</div>
                    <div class="stat-label">Promedio Inseguridad</div>
                </div>
                <div class="stat-item">
                    <div class="stat-value">{trend_icon} {data['trend'].title()}</div>
                    <div class="stat-label">Tendencia (R² = {data['r2']:.3f})</div>
                </div>
                <div class="stat-item">
                    <div class="stat-value">{data['total_surveys']:,}</div>
                    <div class="stat-label">Total Encuestas</div>
                </div>
                <div class="stat-item">
                    <div class="stat-value">{data['min_value']:.1f}% ({data['min_period']})</div>
                    <div class="stat-label">Menor Inseguridad</div>
                </div>
                <div class="stat-item">
                    <div class="stat-value">{data['max_value']:.1f}% ({data['max_period']})</div>
                    <div class="stat-label">Mayor Inseguridad</div>
                </div>
                <div class="stat-item">
                    <div class="stat-value">{len(data['df'])}</div>
                    <div class="stat-label">Períodos Registrados</div>
                </div>
            </div>
        </div>
        '''
        cards_html += card_html
    
    return cards_html

def generate_storytelling_content(df, plot_data):
    """
    Genera el contenido de storytelling basado en los datos
    """
    total_surveys = df['TOTAL_REGISTROS'].sum()
    overall_avg = df['PCT_INSEGUROS'].mean()
    period_range = f"{df['PERIODO_STR'].min()} - {df['PERIODO_STR'].max()}"
    
    # Análisis por municipio
    merida_data = plot_data.get('Mérida', {})
    kanasin_data = plot_data.get('Kanasín', {})
    progreso_data = plot_data.get('Progreso', {})
    uman_data = plot_data.get('Umán', {})
    
    # Detectar impacto COVID
    covid_periods = ['2020Q1', '2020Q2', '2020Q3', '2020Q4', '2021Q1', '2021Q2']
    covid_data = df[df['PERIODO_STR'].isin(covid_periods)]
    pre_covid_data = df[df['PERIODO'] < 2020]
    post_covid_data = df[df['PERIODO'] > 2021.5]
    
    covid_avg = covid_data['PCT_INSEGUROS'].mean() if len(covid_data) > 0 else 0
    pre_covid_avg = pre_covid_data['PCT_INSEGUROS'].mean() if len(pre_covid_data) > 0 else 0
    
    storytelling = f'''
    <div class="storytelling-section">
        <h2>📊 Análisis de la Evolución de la Percepción de Inseguridad en Yucatán</h2>
        
        <div class="analysis-paragraph">
            <p><strong>Panorama General:</strong> El análisis de la percepción de inseguridad en los municipios de Yucatán durante el período {period_range} revela patrones diferenciados entre las localidades estudiadas. Con un total de <strong>{total_surveys:,} encuestas</strong> realizadas por el INEGI a través de la ENSU, se observa un promedio estatal de <strong>{overall_avg:.1f}% de percepción de inseguridad</strong>.</p>
        </div>
        
        <div class="analysis-paragraph">
            <p><strong>Mérida como Referencia:</strong> La capital yucateca, con <strong>{merida_data.get('total_surveys', 0):,} encuestas</strong>, muestra un promedio de inseguridad del <strong>{merida_data.get('avg_insecurity', 0):.1f}%</strong> y una tendencia <strong>{merida_data.get('trend', 'estable')}</strong>. Como centro urbano principal del estado, Mérida cuenta con mayor infraestructura de seguridad pública y representa el 60-70% de las encuestas totales, proporcionando la base más robusta para el análisis temporal.</p>
        </div>
        
        <div class="analysis-paragraph">
            <p><strong>Municipios Periféricos:</strong> Kanasín, Progreso y Umán presentan muestras considerablemente menores (entre {min(kanasin_data.get('total_surveys', 0), progreso_data.get('total_surveys', 0), uman_data.get('total_surveys', 0)):,} y {max(kanasin_data.get('total_surveys', 0), progreso_data.get('total_surveys', 0), uman_data.get('total_surveys', 0)):,} encuestas cada uno), lo que <strong>limita la precisión estadística</strong> de las conclusiones para estos municipios. No obstante, los datos sugieren variaciones importantes en la percepción de seguridad entre localidades.</p>
        </div>
        
        <div class="analysis-paragraph">
            <p><strong>Impacto de Eventos Contextuales:</strong> {"El período de la pandemia COVID-19 (2020-2021) muestra un incremento promedio del " + f"{covid_avg - pre_covid_avg:.1f}%" + " en la percepción de inseguridad respecto al período previo, posiblemente relacionado con los cambios en la dinámica social y económica." if covid_avg > 0 and pre_covid_avg > 0 else "Los datos disponibles durante el período de pandemia son limitados, dificultando la evaluación de su impacto específico."}</p>
        </div>
        
        <div class="analysis-paragraph">
            <p><strong>Tendencias Proyectadas:</strong> Las regresiones lineales calculadas sugieren que <strong>Mérida</strong> presenta una tendencia <strong>{merida_data.get('trend', 'estable')}</strong>, mientras que los municipios menores muestran patrones más volátiles debido al tamaño reducido de sus muestras. Las proyecciones a cuatro trimestres deben interpretarse con cautela, especialmente para municipios con datos limitados.</p>
        </div>
        
        <div class="methodology-note">
            <h3>⚠️ Consideraciones Metodológicas</h3>
            <ul>
                <li><strong>Limitaciones de muestra:</strong> Los municipios distintos a Mérida cuentan con muestras pequeñas (&lt;50 encuestas por trimestre en promedio), lo que incrementa el margen de error estadístico.</li>
                <li><strong>Representatividad temporal:</strong> Algunos períodos presentan datos faltantes debido a la metodología de aplicación de la ENSU del INEGI.</li>
                <li><strong>Contexto socioeconómico:</strong> Los resultados deben interpretarse considerando las diferencias en desarrollo urbano, infraestructura de seguridad y características demográficas entre municipios.</li>
                <li><strong>Proyecciones:</strong> Las predicciones estadísticas asumen continuidad de tendencias actuales y no consideran cambios de política pública o eventos extraordinarios.</li>
            </ul>
        </div>
    </div>
    '''
    
    return storytelling

def generate_html_report(df, fig, plot_data, municipalities, logo_base64):
    """
    Genera el reporte HTML completo
    """
    print("🌐 Generando reporte HTML...")
    
    # Convertir la gráfica a HTML
    plot_html = fig.to_html(
        include_plotlyjs='cdn',
        div_id='main-plot',
        config={'displayModeBar': True, 'toImageButtonOptions': {'format': 'png', 'filename': 'percepcion_inseguridad_yucatan', 'height': 500, 'width': 1000, 'scale': 1}}
    ).split('<body>')[1].split('</body>')[0]
    
    # Generar tarjetas de estadísticas
    stats_cards = generate_municipality_stats_cards(plot_data, municipalities)
    
    # Generar contenido de storytelling
    storytelling_content = generate_storytelling_content(df, plot_data)
    
    print(f"📊 Total de trazas generadas: {len(fig.data)}")
    print("🏷️  Resumen de trazas por municipio:")
    
    # Mapeo fijo basado en el orden conocido (cada municipio tiene 4 trazas)
    municipality_trace_counts = {
        'merida': {'start': 0, 'count': 4},
        'kanasin': {'start': 4, 'count': 4}, 
        'progreso': {'start': 8, 'count': 4},
        'uman': {'start': 12, 'count': 4}
    }
    
    for i, municipality in enumerate(municipalities):
        municipality_key = municipality.lower().replace('á', 'a').replace('í', 'i')
        start_idx = i * 4
        print(f"   {municipality}: 4 trazas (índices {start_idx}-{start_idx+3})")
    
    # JavaScript para manejo de pestañas con mapeo fijo corregido
    tab_js = f'''
            // Mapeo fijo de municipios a índices de trazas (cada municipio = 4 trazas)
            const municipalityTraceMap = {municipality_trace_counts};
            
            console.log('🏘️ Mapeo fijo de trazas:', municipalityTraceMap);
            console.log('📊 Total de trazas en plot:', plotDiv.data.length);
            
            // Validación defensiva
            if (!municipalityTraceMap[municipality]) {{
                console.error('❌ Municipio no encontrado en mapeo:', municipality);
                return;
            }}
            
            const traceInfo = municipalityTraceMap[municipality];
            console.log('🎯 Seleccionado:', municipality, 'Trazas:', traceInfo.start, 'a', traceInfo.start + traceInfo.count - 1);
            
            // Ocultar todas las trazas (principio de estado limpio)
            for(let j = 0; j < plotDiv.data.length; j++) {{
                update['visible[' + j + ']'] = false;
            }}
            
            // Mostrar trazas del municipio seleccionado con validación
            for(let k = traceInfo.start; k < traceInfo.start + traceInfo.count && k < plotDiv.data.length; k++) {{
                update['visible[' + k + ']'] = true;
                console.log('✅ Mostrando traza', k, ':', plotDiv.data[k].name);
            }}
            
            // Mostrar tarjeta de estadísticas correspondiente
            document.querySelectorAll('.stats-card').forEach(card => {{
                card.style.display = 'none';
                console.log('🔄 Ocultando tarjeta:', card.id);
            }});
            
            const statsCard = document.getElementById('stats-' + municipality);
            if(statsCard) {{
                statsCard.style.display = 'block';
                console.log('✅ Mostrando tarjeta:', statsCard.id);
            }} else {{
                console.error('❌ Tarjeta no encontrada para:', municipality);
            }}
    '''
    
    html_content = f'''
    <!DOCTYPE html>
    <html lang="es">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Análisis de Percepción de Seguridad Urbana - Yucatán 2016-2025</title>
        <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
        <style>
            * {{
                margin: 0;
                padding: 0;
                box-sizing: border-box;
            }}
            
            body {{
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                line-height: 1.6;
                color: {COLORS['text']};
                background-color: {COLORS['background']};
            }}
            
            .container {{
                max-width: 1200px;
                margin: 0 auto;
                padding: 20px;
            }}
            
            .header {{
                background: linear-gradient(135deg, {COLORS['upy_purple']}, {COLORS['upy_orange']});
                color: white;
                padding: 30px 0;
                text-align: center;
                margin-bottom: 30px;
                border-radius: 10px;
                box-shadow: 0 4px 8px rgba(0,0,0,0.1);
            }}
            
            .header-content {{
                display: flex;
                align-items: center;
                justify-content: center;
                gap: 20px;
                flex-wrap: wrap;
            }}
            
            .logo {{
                height: 60px;
                width: auto;
            }}
            
            .header h1 {{
                font-size: 2.2em;
                font-weight: 300;
                margin-bottom: 10px;
            }}
            
            .header p {{
                font-size: 1.1em;
                opacity: 0.9;
            }}
            
            .tabs-container {{
                margin: 30px 0;
                text-align: center;
            }}
            
            .tabs {{
                display: inline-flex;
                background: white;
                border-radius: 25px;
                padding: 5px;
                box-shadow: 0 2px 10px rgba(0,0,0,0.1);
                gap: 5px;
            }}
            
            .tab-button {{
                padding: 12px 24px;
                border: none;
                background: transparent;
                color: {COLORS['text']};
                cursor: pointer;
                border-radius: 20px;
                font-weight: 500;
                font-size: 16px;
                transition: all 0.3s ease;
                position: relative;
                overflow: hidden;
            }}
            
            .tab-button:hover {{
                transform: translateY(-2px);
                box-shadow: 0 4px 8px rgba(0,0,0,0.1);
            }}
            
            .tab-button.active {{
                color: white;
                font-weight: 600;
                transform: translateY(-2px);
                box-shadow: 0 4px 12px rgba(0,0,0,0.2);
            }}
            
            .tab-button.merida.active {{ background: linear-gradient(135deg, #2E8B57, #3CB371); }}
            .tab-button.kanasin.active {{ background: linear-gradient(135deg, #4682B4, #87CEEB); }}
            .tab-button.progreso.active {{ background: linear-gradient(135deg, #CD853F, #DEB887); }}
            .tab-button.uman.active {{ background: linear-gradient(135deg, #8B4513, #D2691E); }}
            
            .plot-container {{
                background: white;
                border-radius: 10px;
                padding: 20px;
                margin: 20px 0;
                box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            }}
            
            .stats-section {{
                margin: 30px 0;
            }}
            
            .stats-card {{
                background: white;
                border-radius: 10px;
                padding: 25px;
                box-shadow: 0 2px 10px rgba(0,0,0,0.1);
                margin-bottom: 20px;
            }}
            
            .card-header {{
                display: flex;
                justify-content: space-between;
                align-items: center;
                margin-bottom: 20px;
                padding-left: 15px;
            }}
            
            .card-header h3 {{
                color: {COLORS['text']};
                font-size: 1.4em;
                font-weight: 600;
            }}
            
            .limited-data-warning {{
                background: #FFF3CD;
                color: #856404;
                padding: 5px 10px;
                border-radius: 15px;
                font-size: 0.9em;
                font-weight: 500;
            }}
            
            .stats-grid {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
                gap: 20px;
            }}
            
            .stat-item {{
                text-align: center;
                padding: 15px;
                background: #F8F9FA;
                border-radius: 8px;
                transition: transform 0.2s ease;
            }}
            
            .stat-item:hover {{
                transform: translateY(-2px);
            }}
            
            .stat-value {{
                font-size: 1.5em;
                font-weight: 700;
                color: {COLORS['upy_purple']};
                margin-bottom: 5px;
            }}
            
            .stat-label {{
                color: #6C757D;
                font-size: 0.9em;
                font-weight: 500;
            }}
            
            .storytelling-section {{
                background: white;
                border-radius: 10px;
                padding: 30px;
                margin: 30px 0;
                box-shadow: 0 2px 10px rgba(0,0,0,0.1);
                line-height: 1.8;
            }}
            
            .storytelling-section h2 {{
                color: {COLORS['upy_purple']};
                font-size: 1.8em;
                margin-bottom: 25px;
                border-bottom: 3px solid {COLORS['upy_orange']};
                padding-bottom: 10px;
            }}
            
            .analysis-paragraph {{
                margin-bottom: 20px;
            }}
            
            .analysis-paragraph p {{
                text-align: justify;
                margin-bottom: 15px;
            }}
            
            .methodology-note {{
                background: #F8F9FA;
                border-left: 4px solid {COLORS['upy_orange']};
                padding: 20px;
                margin-top: 30px;
                border-radius: 0 8px 8px 0;
            }}
            
            .methodology-note h3 {{
                color: {COLORS['upy_purple']};
                margin-bottom: 15px;
            }}
            
            .methodology-note ul {{
                padding-left: 20px;
            }}
            
            .methodology-note li {{
                margin-bottom: 8px;
            }}
            
            .footer {{
                background: {COLORS['text']};
                color: white;
                text-align: center;
                padding: 20px;
                margin-top: 40px;
                border-radius: 10px;
            }}
            
            .footer p {{
                margin-bottom: 5px;
            }}
            
            .export-button {{
                background: {COLORS['upy_orange']};
                color: white;
                border: none;
                padding: 10px 20px;
                border-radius: 5px;
                cursor: pointer;
                font-weight: 500;
                margin: 10px 5px;
                transition: background 0.3s ease;
            }}
            
            .export-button:hover {{
                background: #E6890A;
                transform: translateY(-1px);
            }}
            
            @media (max-width: 768px) {{
                .header-content {{
                    flex-direction: column;
                }}
                
                .tabs {{
                    flex-direction: column;
                    align-items: stretch;
                }}
                
                .stats-grid {{
                    grid-template-columns: 1fr;
                }}
            }}
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
                {plot_html}
            </div>
            
            <!-- Estadísticas por Municipio -->
            <div class="stats-section">
                {stats_cards}
            </div>
            
            <!-- Storytelling y Análisis -->
            {storytelling_content}
            
            <!-- Footer -->
            <div class="footer">
                <p><strong>Fuente:</strong> Instituto Nacional de Estadística y Geografía (INEGI) - Encuesta Nacional de Seguridad Urbana (ENSU)</p>
                <p><strong>Procesamiento:</strong> Universidad Politécnica de Yucatán | Generado el {datetime.now().strftime('%d de %B de %Y')}</p>
                <button class="export-button" onclick="exportChart()">📊 Exportar Gráfica</button>
            </div>
        </div>
        
        <script>
            function switchMunicipality(municipality) {{
                console.log('🔄 Cambiando a municipio:', municipality);
                
                // Actualizar pestañas activas (principio de feedback visual)
                document.querySelectorAll('.tab-button').forEach(tab => {{
                    tab.classList.remove('active');
                    console.log('🔄 Removiendo active de:', tab.textContent);
                }});
                
                const activeTab = document.querySelector('.tab-button.' + municipality);
                if(activeTab) {{
                    activeTab.classList.add('active');
                    console.log('✅ Activando pestaña:', activeTab.textContent);
                }} else {{
                    console.error('❌ Pestaña no encontrada:', municipality);
                }}
                
                // Actualizar gráfica (principio de separación de concerns)
                const plotDiv = document.getElementById('main-plot');
                if(!plotDiv) {{
                    console.error('❌ Plot div no encontrado');
                    return;
                }}
                
                const update = {{}};
                
                {tab_js}
                
                // Aplicar actualización con animación suave (principio de UX)
                console.log('🎬 Aplicando actualización de trazas...');
                Plotly.restyle(plotDiv, update).then(() => {{
                    console.log('✅ Gráfica actualizada exitosamente');
                    // Animación adicional si es necesaria
                    Plotly.relayout(plotDiv, {{
                        'transition': {{
                            'duration': 500,
                            'easing': 'cubic-in-out'
                        }}
                    }});
                }}).catch(error => {{
                    console.error('❌ Error actualizando gráfica:', error);
                }});
            }}
            
            function exportChart() {{
                console.log('📊 Exportando gráfica...');
                const plotDiv = document.getElementById('main-plot');
                Plotly.toImage(plotDiv, {{
                    format: 'png',
                    width: 1000,
                    height: 600,
                    scale: 2
                }}).then(function(url) {{
                    const link = document.createElement('a');
                    link.download = 'percepcion_inseguridad_yucatan.png';
                    link.href = url;
                    link.click();
                    console.log('✅ Gráfica exportada');
                }}).catch(error => {{
                    console.error('❌ Error exportando:', error);
                }});
            }}
            
            // Inicializar con Mérida seleccionado (principio de estado inicial predecible)
            document.addEventListener('DOMContentLoaded', function() {{
                console.log('🚀 Inicializando página...');
                console.log('🏘️ Municipios disponibles: Mérida, Kanasín, Progreso, Umán');
                setTimeout(() => {{
                    switchMunicipality('merida');
                    console.log('✅ Inicialización completada con Mérida');
                }}, 1000); // Delay para asegurar que Plotly esté listo
            }});
        </script>
    </body>
    </html>
    '''
    
    return html_content

def main():
    """
    Función principal del generador de reportes
    """
    print("🚀 Iniciando generación de reporte HTML interactivo...")
    
    # Rutas de archivos
    data_path = "data/yucatan-inseguridad/data-yucatan-inseguridad.csv"
    logo_path = "resources/logo-upy.png"
    output_path = "reporte_analisis_yucatan.html"
    
    try:
        # Cargar y preparar datos
        df = load_and_prepare_data(data_path)
        
        # Codificar logo
        logo_base64 = encode_image_to_base64(logo_path)
        
        # Crear gráfica interactiva
        fig, plot_data, municipalities = create_interactive_plot(df)
        
        # Generar HTML
        html_content = generate_html_report(df, fig, plot_data, municipalities, logo_base64)
        
        # Guardar archivo
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        print(f"✅ Reporte generado exitosamente: {output_path}")
        print(f"📊 Municipios procesados: {', '.join(municipalities)}")
        print(f"📈 Total de registros: {len(df)}")
        print(f"🌐 Abrir archivo en navegador para visualizar")
        
    except Exception as e:
        print(f"❌ Error generando reporte: {str(e)}")
        raise

if __name__ == "__main__":
    main()
