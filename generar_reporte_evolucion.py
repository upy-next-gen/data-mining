#!/usr/bin/env python3
"""
Script para generar reporte HTML de evolución de percepción de inseguridad en Yucatán
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')  # Backend no interactivo
import base64
from io import BytesIO
import logging
from datetime import datetime
import re
from pathlib import Path
import glob
import warnings
warnings.filterwarnings('ignore')

def configurar_logging():
    """Configura el sistema de logging para el análisis"""
    # Crear carpeta de reportes si no existe
    reportes_dir = Path("data/yucatan-inseguridad/reportes")
    reportes_dir.mkdir(parents=True, exist_ok=True)
    
    # Configurar logging
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = reportes_dir / f"log_analisis_yucatan_{timestamp}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    
    return logging.getLogger(__name__)

def cargar_datos_yucatan():
    """Carga todos los archivos procesados y filtra solo Yucatán"""
    logger = logging.getLogger(__name__)
    logger.info("Cargando datos de Yucatán...")
    
    archivos = glob.glob("data/yucatan-inseguridad/procesado_*.csv")
    archivos.sort()
    
    logger.info(f"Archivos encontrados: {len(archivos)}")
    
    dfs = []
    for archivo in archivos:
        df = pd.read_csv(archivo)
        # Filtrar solo Yucatán
        df_yuc = df[df['NOM_ENT'] == 'YUCATAN'].copy()
        if not df_yuc.empty:
            # Crear columna de periodo para ordenamiento
            df_yuc['PERIODO'] = df_yuc['AÑO'].astype(str) + '-' + df_yuc['TRIMESTRE']
            dfs.append(df_yuc)
            logger.info(f"  {Path(archivo).name}: {len(df_yuc)} municipios")
    
    if dfs:
        datos_completos = pd.concat(dfs, ignore_index=True)
        logger.info(f"Total de registros de Yucatán: {len(datos_completos)}")
        return datos_completos
    else:
        logger.error("No se encontraron datos de Yucatán")
        return pd.DataFrame()

def corregir_trimestres(df):
    """Detecta y corrige trimestres duplicados"""
    logger = logging.getLogger(__name__)
    logger.info("Verificando trimestres...")
    
    df = df.copy()
    correcciones = []
    
    # Agrupar por año y verificar duplicados
    for año in df['AÑO'].unique():
        datos_año = df[df['AÑO'] == año]
        trimestres_unicos = datos_año['TRIMESTRE'].unique()
        
        # Contar ocurrencias de cada trimestre
        conteo = datos_año.groupby('TRIMESTRE').size()
        
        if len(conteo) < 4 and año not in ['2016', '2020', '2025']:
            logger.warning(f"Año {año}: Faltan trimestres. Encontrados: {list(trimestres_unicos)}")
        
        # Verificar si hay inconsistencias
        municipios_por_trimestre = datos_año.groupby('TRIMESTRE')['NOM_MUN'].count()
        if municipios_por_trimestre.std() > 10:
            logger.info(f"Año {año}: Variación en número de municipios por trimestre")
    
    # Mapeo manual de correcciones conocidas basado en análisis previo
    correcciones_manuales = {
        ('2017', 'Q1', 2): 'Q2',  # Segundo Q1 de 2017 es Q2
        ('2017', 'Q1', 3): 'Q3',  # Tercer Q1 de 2017 es Q3
        ('2017', 'Q2', 2): 'Q4',  # Segundo Q2 de 2017 es Q4
        ('2018', 'Q1', 2): 'Q2',  # Segundo Q1 de 2018 es Q2
        ('2018', 'Q1', 3): 'Q3',  # Tercer Q1 de 2018 es Q3
        ('2018', 'Q2', 2): 'Q4',  # Segundo Q2 de 2018 es Q4
        ('2019', 'Q2', 2): 'Q4',  # Segundo Q2 de 2019 es Q4
    }
    
    # Aplicar correcciones
    for año in df['AÑO'].unique():
        datos_año = df[df['AÑO'] == año]
        
        for trimestre in ['Q1', 'Q2', 'Q3', 'Q4']:
            datos_trim = datos_año[datos_año['TRIMESTRE'] == trimestre]
            
            if len(datos_trim) > 0:
                # Si hay múltiples grupos del mismo trimestre
                grupos = datos_trim.groupby(['TOTAL_REGISTROS']).size()
                
                if len(grupos) > 1:
                    # Ordenar por total de registros para identificar diferentes encuestas
                    grupos_ordenados = datos_trim.sort_values('TOTAL_REGISTROS')
                    
                    contador = 1
                    for idx, row in grupos_ordenados.iterrows():
                        clave = (str(año), trimestre, contador)
                        if clave in correcciones_manuales:
                            nuevo_trimestre = correcciones_manuales[clave]
                            df.at[idx, 'TRIMESTRE'] = nuevo_trimestre
                            df.at[idx, 'PERIODO'] = f"{año}-{nuevo_trimestre}"
                            correcciones.append(f"Corregido: {año}-{trimestre} → {año}-{nuevo_trimestre}")
                            logger.info(f"  Corregido: {año}-{trimestre} → {año}-{nuevo_trimestre}")
                        contador += 1
    
    # Actualizar periodo después de correcciones
    df['PERIODO'] = df['AÑO'].astype(str) + '-' + df['TRIMESTRE']
    
    if correcciones:
        logger.info(f"Total de correcciones aplicadas: {len(correcciones)}")
    
    return df

def calcular_promedio_estatal(df):
    """Calcula el promedio ponderado estatal por periodo"""
    logger = logging.getLogger(__name__)
    logger.info("Calculando promedio estatal ponderado...")
    
    resultado = []
    
    periodos = df['PERIODO'].unique()
    periodos = sorted(periodos)
    
    for periodo in periodos:
        datos_periodo = df[df['PERIODO'] == periodo]
        
        total_inseguros = datos_periodo['TOTAL_INSEGUROS'].sum()
        total_registros = datos_periodo['TOTAL_REGISTROS'].sum()
        
        if total_registros > 0:
            pct_inseguros = (total_inseguros / total_registros) * 100
            resultado.append({
                'PERIODO': periodo,
                'PCT_INSEGUROS': pct_inseguros,
                'TOTAL_REGISTROS': total_registros,
                'NUM_MUNICIPIOS': len(datos_periodo)
            })
    
    df_estatal = pd.DataFrame(resultado)
    logger.info(f"  Periodos con datos: {len(df_estatal)}")
    logger.info(f"  Promedio general: {df_estatal['PCT_INSEGUROS'].mean():.2f}%")
    
    return df_estatal

def identificar_gaps(df):
    """Identifica municipios con datos faltantes"""
    logger = logging.getLogger(__name__)
    logger.info("Identificando datos faltantes...")
    
    # Obtener todos los periodos y municipios únicos
    periodos = sorted(df['PERIODO'].unique())
    municipios = sorted(df['NOM_MUN'].unique())
    
    gaps = []
    
    for municipio in municipios:
        datos_mun = df[df['NOM_MUN'] == municipio]
        periodos_mun = set(datos_mun['PERIODO'].unique())
        periodos_faltantes = set(periodos) - periodos_mun
        
        if periodos_faltantes:
            gaps.append({
                'municipio': municipio,
                'periodos_presentes': len(periodos_mun),
                'periodos_totales': len(periodos),
                'faltantes': sorted(list(periodos_faltantes))
            })
            
            if len(periodos_faltantes) <= 5:  # Solo log si son pocos
                logger.info(f"  {municipio}: Faltan {periodos_faltantes}")
    
    logger.info(f"Total de municipios con datos faltantes: {len(gaps)}")
    
    return gaps

def generar_grafico_estatal(df_estatal):
    """Genera gráfico de evolución estatal"""
    logger = logging.getLogger(__name__)
    logger.info("Generando gráfico estatal...")
    
    fig, ax = plt.subplots(figsize=(10, 6.25))  # 800x500 px at 80 dpi
    
    # Preparar datos
    df_estatal = df_estatal.sort_values('PERIODO')
    x = range(len(df_estatal))
    y = df_estatal['PCT_INSEGUROS'].values
    
    # Determinar color según nivel promedio
    promedio = y.mean()
    if promedio < 30:
        color = 'green'
    elif promedio < 60:
        color = 'orange'
    else:
        color = 'red'
    
    # Graficar
    ax.plot(x, y, marker='o', linewidth=2.5, markersize=8, color=color)
    
    # Configurar ejes
    ax.set_xticks(x)
    ax.set_xticklabels(df_estatal['PERIODO'].values, rotation=45, ha='right')
    ax.set_xlabel('Periodo', fontsize=12)
    ax.set_ylabel('Porcentaje de Percepción Insegura (%)', fontsize=12)
    ax.set_title('Evolución de la Percepción de Inseguridad en Yucatán\n(Promedio Estatal Ponderado)', 
                 fontsize=14, fontweight='bold')
    
    # Grid y límites
    ax.grid(True, alpha=0.3)
    ax.set_ylim(0, 100)
    
    # Agregar línea de promedio
    ax.axhline(y=promedio, color='gray', linestyle='--', alpha=0.5, 
               label=f'Promedio: {promedio:.1f}%')
    ax.legend()
    
    plt.tight_layout()
    
    # Convertir a base64
    buffer = BytesIO()
    plt.savefig(buffer, format='png', dpi=80, bbox_inches='tight')
    buffer.seek(0)
    img_base64 = base64.b64encode(buffer.read()).decode()
    plt.close()
    
    logger.info(f"  Gráfico estatal generado. Promedio histórico: {promedio:.2f}%")
    
    return img_base64

def generar_graficos_municipales(df):
    """Genera gráficos individuales por municipio"""
    logger = logging.getLogger(__name__)
    logger.info("Generando gráficos municipales...")
    
    # Calcular promedio histórico por municipio
    promedios_mun = df.groupby('NOM_MUN').agg({
        'PCT_INSEGUROS': 'mean',
        'TOTAL_REGISTROS': 'sum'
    }).reset_index()
    
    # Ordenar por nivel de inseguridad (mayor a menor)
    promedios_mun = promedios_mun.sort_values('PCT_INSEGUROS', ascending=False)
    
    graficos = []
    
    for _, mun_info in promedios_mun.iterrows():
        municipio = mun_info['NOM_MUN']
        promedio = mun_info['PCT_INSEGUROS']
        
        # Datos del municipio
        datos_mun = df[df['NOM_MUN'] == municipio].sort_values('PERIODO')
        
        if len(datos_mun) == 0:
            continue
        
        fig, ax = plt.subplots(figsize=(10, 6.25))
        
        # Preparar datos - mantener gaps
        periodos_completos = sorted(df['PERIODO'].unique())
        valores = []
        periodos_con_datos = []
        
        for periodo in periodos_completos:
            dato = datos_mun[datos_mun['PERIODO'] == periodo]
            if not dato.empty:
                valores.append(dato['PCT_INSEGUROS'].values[0])
                periodos_con_datos.append(periodo)
            else:
                valores.append(None)  # Gap
        
        # Determinar color
        if promedio < 30:
            color = 'green'
        elif promedio < 60:
            color = 'orange'
        else:
            color = 'red'
        
        # Graficar con gaps
        x = range(len(periodos_completos))
        ax.plot(x, valores, marker='o', linewidth=2, markersize=6, color=color)
        
        # Configurar ejes
        ax.set_xticks(x)
        ax.set_xticklabels(periodos_completos, rotation=45, ha='right', fontsize=9)
        ax.set_xlabel('Periodo', fontsize=11)
        ax.set_ylabel('Porcentaje de Percepción Insegura (%)', fontsize=11)
        ax.set_title(f'Municipio: {municipio} - Evolución de Percepción de Inseguridad\nPromedio histórico: {promedio:.1f}%', 
                    fontsize=13, fontweight='bold')
        
        # Grid y límites
        ax.grid(True, alpha=0.3)
        ax.set_ylim(0, 100)
        
        # Línea de promedio municipal
        ax.axhline(y=promedio, color='gray', linestyle='--', alpha=0.5)
        
        plt.tight_layout()
        
        # Convertir a base64
        buffer = BytesIO()
        plt.savefig(buffer, format='png', dpi=80, bbox_inches='tight')
        buffer.seek(0)
        img_base64 = base64.b64encode(buffer.read()).decode()
        plt.close()
        
        graficos.append({
            'municipio': municipio,
            'promedio': promedio,
            'grafico': img_base64,
            'num_periodos': len(periodos_con_datos)
        })
        
        logger.info(f"  {municipio}: {promedio:.1f}% (promedio), {len(periodos_con_datos)} periodos")
    
    logger.info(f"Total de gráficos municipales generados: {len(graficos)}")
    
    return graficos

def crear_tabla_completa(df):
    """Crea tabla pivote con todos los datos"""
    logger = logging.getLogger(__name__)
    logger.info("Creando tabla de datos completa...")
    
    # Crear tabla pivote
    tabla = df.pivot_table(
        index='NOM_MUN',
        columns='PERIODO',
        values='PCT_INSEGUROS',
        aggfunc='first'
    )
    
    # Ordenar columnas cronológicamente
    columnas_ordenadas = sorted(tabla.columns)
    tabla = tabla[columnas_ordenadas]
    
    # Calcular promedio por municipio
    tabla['PROMEDIO'] = tabla.mean(axis=1)
    
    # Ordenar por promedio (mayor a menor)
    tabla = tabla.sort_values('PROMEDIO', ascending=False)
    
    logger.info(f"  Tabla creada: {tabla.shape[0]} municipios x {tabla.shape[1]-1} periodos")
    
    return tabla

def generar_html(grafico_estatal, graficos_municipales, tabla, gaps, correcciones):
    """Genera el reporte HTML completo"""
    logger = logging.getLogger(__name__)
    logger.info("Generando reporte HTML...")
    
    # Preparar tabla HTML
    tabla_html = tabla.copy()
    
    # Formatear valores con 1 decimal
    for col in tabla_html.columns:
        if col != 'PROMEDIO':
            tabla_html[col] = tabla_html[col].apply(lambda x: f"{x:.1f}" if pd.notna(x) else "-")
        else:
            tabla_html[col] = tabla_html[col].apply(lambda x: f"<b>{x:.1f}</b>" if pd.notna(x) else "-")
    
    # Convertir a HTML
    tabla_html_str = tabla_html.to_html(escape=False, classes='tabla')
    tabla_html_str = tabla_html_str.replace('<th>', '<th style="position: sticky; top: 0; background: white;">')
    
    # Generar lista de municipios para índice
    indices_municipios = '\n'.join([
        f'<li><a href="#mun_{i}">{g["municipio"]} ({g["promedio"]:.1f}%)</a></li>'
        for i, g in enumerate(graficos_municipales)
    ])
    
    # Generar sección de gráficos municipales
    graficos_html = '\n'.join([
        f'''
        <div class="grafico" id="mun_{i}">
            <h3>{g["municipio"]}</h3>
            <p>Promedio histórico: <strong>{g["promedio"]:.1f}%</strong> | 
               Periodos con datos: {g["num_periodos"]}</p>
            <img src="data:image/png;base64,{g["grafico"]}" alt="Gráfico {g["municipio"]}">
        </div>
        '''
        for i, g in enumerate(graficos_municipales)
    ])
    
    # Notas sobre gaps
    notas_gaps = ""
    if gaps:
        municipios_con_gaps = [g['municipio'] for g in gaps if len(g['faltantes']) <= 5]
        if municipios_con_gaps:
            notas_gaps = f"<li>Municipios con datos faltantes: {', '.join(municipios_con_gaps[:10])}"
            if len(municipios_con_gaps) > 10:
                notas_gaps += f" y {len(municipios_con_gaps)-10} más"
            notas_gaps += "</li>"
    
    # HTML completo
    html_content = f'''<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Reporte de Evolución - Percepción de Inseguridad en Yucatán</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif;
            margin: 0;
            padding: 40px;
            background: #f5f5f5;
            color: #333;
        }}
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            background: white;
            padding: 40px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        h1 {{
            color: #2c3e50;
            border-bottom: 3px solid #3498db;
            padding-bottom: 10px;
        }}
        h2 {{
            color: #34495e;
            margin-top: 40px;
            border-bottom: 1px solid #ecf0f1;
            padding-bottom: 5px;
        }}
        h3 {{
            color: #7f8c8d;
            margin-top: 30px;
        }}
        .metadata {{
            background: #ecf0f1;
            padding: 15px;
            border-radius: 5px;
            margin: 20px 0;
        }}
        .metadata p {{
            margin: 5px 0;
        }}
        .indice {{
            background: #f8f9fa;
            padding: 20px;
            border-radius: 5px;
            margin: 20px 0;
        }}
        .indice ul {{
            columns: 2;
            column-gap: 40px;
        }}
        .indice li {{
            margin: 5px 0;
        }}
        .grafico {{
            margin: 40px 0;
            text-align: center;
            padding: 20px;
            border: 1px solid #ecf0f1;
            border-radius: 5px;
        }}
        .grafico img {{
            max-width: 100%;
            height: auto;
            border: 1px solid #ddd;
            border-radius: 3px;
        }}
        .tabla {{
            width: 100%;
            border-collapse: collapse;
            margin: 30px 0;
            font-size: 14px;
        }}
        .tabla th {{
            background: #3498db;
            color: white;
            padding: 10px;
            text-align: center;
            border: 1px solid #2980b9;
        }}
        .tabla td {{
            padding: 8px;
            text-align: center;
            border: 1px solid #ecf0f1;
        }}
        .tabla tr:hover {{
            background: #f8f9fa;
        }}
        .tabla-container {{
            overflow-x: auto;
            margin: 30px 0;
        }}
        .notas {{
            background: #fff3cd;
            border-left: 4px solid #ffc107;
            padding: 15px;
            margin: 30px 0;
        }}
        .notas h3 {{
            margin-top: 0;
            color: #856404;
        }}
        a {{
            color: #3498db;
            text-decoration: none;
        }}
        a:hover {{
            text-decoration: underline;
        }}
        .volver {{
            position: fixed;
            bottom: 20px;
            right: 20px;
            background: #3498db;
            color: white;
            padding: 10px 15px;
            border-radius: 50px;
            text-decoration: none;
            box-shadow: 0 2px 10px rgba(0,0,0,0.2);
        }}
    </style>
</head>
<body>
    <div class="container">
        <!-- Encabezado -->
        <h1>Evolución de la Percepción de Inseguridad en Yucatán</h1>
        
        <div class="metadata">
            <p><strong>Fecha de generación:</strong> {datetime.now().strftime('%d de %B de %Y, %H:%M:%S')}</p>
            <p><strong>Periodo analizado:</strong> 2016-Q2 a 2025-Q2</p>
            <p><strong>Fuente:</strong> Encuesta Nacional de Seguridad Urbana (ENSU) - INEGI</p>
            <p><strong>Metodología:</strong> Promedio estatal calculado con ponderación por número de encuestados</p>
        </div>
        
        <!-- Índice -->
        <h2>Contenido</h2>
        <div class="indice">
            <ul style="columns: 1;">
                <li><a href="#estatal">📊 Promedio Estatal de Yucatán</a></li>
                <li><a href="#municipios">📈 Análisis por Municipio ({len(graficos_municipales)} municipios)</a></li>
                <li><a href="#tabla">📋 Tabla de Datos Completa</a></li>
                <li><a href="#notas">📝 Notas Metodológicas</a></li>
            </ul>
        </div>
        
        <!-- Gráfico Estatal -->
        <h2 id="estatal">Promedio Estatal de Yucatán</h2>
        <div class="grafico">
            <img src="data:image/png;base64,{grafico_estatal}" alt="Gráfico Estatal">
            <p><em>El promedio estatal utiliza ponderación por número de encuestados en cada municipio</em></p>
        </div>
        
        <!-- Gráficos Municipales -->
        <h2 id="municipios">Análisis por Municipio</h2>
        <p>Los municipios están ordenados por su promedio histórico de percepción de inseguridad (de mayor a menor):</p>
        
        <div class="indice">
            <ul>
                {indices_municipios}
            </ul>
        </div>
        
        {graficos_html}
        
        <!-- Tabla de Datos -->
        <h2 id="tabla">Tabla de Datos Completa</h2>
        <p>Porcentaje de percepción de inseguridad por municipio y periodo. Los valores faltantes se indican con "-".</p>
        
        <div class="tabla-container">
            {tabla_html_str}
        </div>
        
        <!-- Notas -->
        <h2 id="notas">Notas Metodológicas</h2>
        <div class="notas">
            <h3>Información importante:</h3>
            <ul>
                <li>Los datos faltantes se muestran como espacios vacíos (gaps) en los gráficos de línea</li>
                <li>El promedio estatal se calcula ponderando por el número de encuestados en cada municipio</li>
                <li>Algunos trimestres fueron corregidos debido a inconsistencias en los nombres de archivo originales</li>
                {notas_gaps}
                <li>Los colores de las líneas indican el nivel promedio de inseguridad: 
                    <span style="color:green">Verde < 30%</span> | 
                    <span style="color:orange">Naranja 30-60%</span> | 
                    <span style="color:red">Rojo > 60%</span>
                </li>
                <li>Total de municipios analizados: {len(graficos_municipales)}</li>
                <li>Total de periodos cubiertos: {len(tabla.columns)-1}</li>
            </ul>
        </div>
        
        <p style="text-align: center; margin-top: 50px; color: #7f8c8d;">
            <small>Reporte generado automáticamente mediante análisis de datos ENSU<br>
            Sistema de Análisis de Percepción de Seguridad v1.0</small>
        </p>
    </div>
    
    <a href="#" class="volver" title="Volver arriba">↑</a>
</body>
</html>'''
    
    # Guardar archivo
    output_file = Path("data/yucatan-inseguridad/reportes/reporte_yucatan_evolucion.html")
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    logger.info(f"  Reporte HTML guardado: {output_file}")
    
    return output_file

def main():
    # Configurar logging
    logger = configurar_logging()
    
    logger.info("="*80)
    logger.info("GENERACIÓN DE REPORTE DE EVOLUCIÓN - PERCEPCIÓN DE INSEGURIDAD EN YUCATÁN")
    logger.info("="*80)
    logger.info(f"Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # 1. Cargar datos
        datos = cargar_datos_yucatan()
        if datos.empty:
            logger.error("No se pudieron cargar datos de Yucatán")
            return
        
        # 2. Corregir trimestres
        datos = corregir_trimestres(datos)
        correcciones = []  # Para el reporte
        
        # 3. Calcular promedio estatal
        promedio_estatal = calcular_promedio_estatal(datos)
        
        # 4. Identificar gaps
        gaps = identificar_gaps(datos)
        
        # 5. Generar gráfico estatal
        grafico_estatal = generar_grafico_estatal(promedio_estatal)
        
        # 6. Generar gráficos municipales
        graficos_municipales = generar_graficos_municipales(datos)
        
        # 7. Crear tabla de datos
        tabla = crear_tabla_completa(datos)
        
        # 8. Generar HTML
        archivo_salida = generar_html(grafico_estatal, graficos_municipales, tabla, gaps, correcciones)
        
        # 9. Resumen final
        logger.info("\n" + "="*80)
        logger.info("RESUMEN DE GENERACIÓN")
        logger.info("="*80)
        logger.info(f"Municipios procesados: {datos['NOM_MUN'].nunique()}")
        logger.info(f"Periodos cubiertos: {datos['PERIODO'].nunique()}")
        logger.info(f"Total de registros: {datos['TOTAL_REGISTROS'].sum():,}")
        logger.info(f"Promedio estatal general: {promedio_estatal['PCT_INSEGUROS'].mean():.2f}%")
        logger.info(f"Municipio más seguro: {tabla.index[-1]} ({tabla.iloc[-1]['PROMEDIO']:.1f}%)")
        logger.info(f"Municipio más inseguro: {tabla.index[0]} ({tabla.iloc[0]['PROMEDIO']:.1f}%)")
        logger.info(f"Archivo generado: {archivo_salida}")
        logger.info(f"Fin: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("="*80)
        
        print(f"\n✓ Reporte generado exitosamente: {archivo_salida}")
        print(f"✓ Log de análisis: data/yucatan-inseguridad/reportes/log_analisis_yucatan_*.log")
        
    except Exception as e:
        logger.error(f"Error en la generación del reporte: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    main()