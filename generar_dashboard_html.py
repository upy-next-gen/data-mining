#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Generador de dashboard HTML profesional para el análisis de percepción de seguridad.
Complementa el script generar_reporte_profesional.py con funciones de visualización HTML.

Autor: Cascade AI
Fecha: 2025-09-10
"""

import os
import sys
import json
import logging
from datetime import datetime
from typing import Dict, List, Any

# Importar funciones del módulo de reporte profesional
from generar_reporte_profesional import (
    setup_logging,
    cargar_datos_procesados,
    calcular_promedio_estatal,
    generar_grafico_tendencia_estatal,
    generar_mapa_calor_municipios,
    generar_comparativa_municipios
)


def generar_dashboard_html(df, df_estatal, resumen, logger):
    """
    Genera un dashboard HTML profesional con todas las visualizaciones.
    """
    # Obtener valores clave
    periodos = sorted(df["PERIODO"].unique(), 
                   key=lambda x: (int(x.split("-")[0]), x.split("-")[1]))
    ultimo_periodo = periodos[-1] if periodos else ""
    promedio_estatal = round((df["TOTAL_INSEGUROS"].sum() / df["TOTAL_REGISTROS"].sum()) * 100, 2)
    
    # Generar imágenes
    logger.info("Generando visualizaciones para el dashboard")
    try:
        grafico_tendencia = generar_grafico_tendencia_estatal(df_estatal, promedio_estatal)
        logger.info("Gráfico de tendencia estatal generado")
        
        mapa_calor = generar_mapa_calor_municipios(df)
        logger.info("Mapa de calor de municipios generado")
        
        comparativa_municipal = generar_comparativa_municipios(df, ultimo_periodo)
        logger.info("Comparativa de municipios generada")
    except Exception as e:
        logger.error(f"Error generando visualizaciones: {str(e)}")
        raise
    
    # Calcular estadísticas para el dashboard
    num_municipios = len(df["NOM_MUN"].unique())
    total_registros = df["TOTAL_REGISTROS"].sum()
    periodos_cubiertos = len(periodos)
    
    # Fecha de generación del reporte
    fecha_generacion = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    rango_periodos = f"{periodos[0]} a {periodos[-1]}" if len(periodos) > 1 else periodos[0]
    
    # Generar tabla de municipios con más inseguridad en el último periodo
    df_ultimo = df[df['PERIODO'] == ultimo_periodo].sort_values('PCT_INSEGUROS', ascending=False)
    tabla_top_municipios = """
        <table class="tabla-top-municipios">
            <thead>
                <tr>
                    <th>Municipio</th>
                    <th>% Inseguridad</th>
                    <th>Estado</th>
                </tr>
            </thead>
            <tbody>
    """
    
    # Generar filas para los 5 municipios más inseguros
    for _, row in df_ultimo.head(5).iterrows():
        nivel = "bajo" if row["PCT_INSEGUROS"] < 30 else "medio" if row["PCT_INSEGUROS"] < 60 else "alto"
        tabla_top_municipios += f"""
            <tr>
                <td>{row["NOM_MUN"]}</td>
                <td class="{nivel}">{row["PCT_INSEGUROS"]:.1f}%</td>
                <td>
                    <div class="indicador-nivel {nivel}"></div>
                </td>
            </tr>
        """
    
    tabla_top_municipios += """
            </tbody>
        </table>
    """
    
    # HTML para las tarjetas KPI
    kpis_html = f"""
    <div class="kpis">
        <div class="kpi-card">
            <div class="kpi-title">Municipios</div>
            <div class="kpi-value">{num_municipios}</div>
            <div class="kpi-subtitle">en el análisis</div>
        </div>
        <div class="kpi-card">
            <div class="kpi-title">Percepción Inseguridad</div>
            <div class="kpi-value {('nivel-bajo' if promedio_estatal < 30 else 'nivel-medio' if promedio_estatal < 60 else 'nivel-alto')}">{promedio_estatal}%</div>
            <div class="kpi-subtitle">promedio estatal</div>
        </div>
        <div class="kpi-card">
            <div class="kpi-title">Periodos</div>
            <div class="kpi-value">{periodos_cubiertos}</div>
            <div class="kpi-subtitle">{rango_periodos}</div>
        </div>
        <div class="kpi-card">
            <div class="kpi-title">Registros</div>
            <div class="kpi-value">{total_registros:,}</div>
            <div class="kpi-subtitle">procesados</div>
        </div>
    </div>
    """
    
    # Generar el HTML completo con el dashboard
    html = f"""
    <!DOCTYPE html>
    <html lang="es">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Dashboard de Percepción de Seguridad - Yucatán</title>
        <style>
            /* Variables CSS */
            :root {{
                --color-primary: #3498db;
                --color-secondary: #2c3e50;
                --color-success: #2ecc71;
                --color-warning: #f1c40f;
                --color-danger: #e74c3c;
                --color-light: #ecf0f1;
                --color-dark: #34495e;
                --color-gray: #95a5a6;
                --shadow-soft: 0 2px 10px rgba(0, 0, 0, 0.1);
                --shadow-medium: 0 5px 15px rgba(0, 0, 0, 0.15);
                --border-radius: 8px;
                --transition-speed: 0.3s;
            }}
            
            /* Estilos globales */
            * {{
                margin: 0;
                padding: 0;
                box-sizing: border-box;
            }}
            
            body {{
                font-family: 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Open Sans', 'Helvetica Neue', sans-serif;
                line-height: 1.6;
                color: var(--color-dark);
                background-color: #f5f7fa;
                padding: 0;
                margin: 0;
            }}
            
            .container {{
                max-width: 1400px;
                margin: 0 auto;
                padding: 0 20px;
            }}
            
            /* Header */
            header {{
                background: linear-gradient(135deg, var(--color-primary), var(--color-secondary));
                color: white;
                padding: 20px 0;
                box-shadow: var(--shadow-medium);
            }}
            
            .header-content {{
                display: flex;
                flex-direction: column;
                align-items: center;
                padding: 20px;
                text-align: center;
            }}
            
            h1 {{
                font-size: 2.5em;
                margin-bottom: 10px;
                font-weight: 700;
            }}
            
            .header-subtitle {{
                font-size: 1.2em;
                opacity: 0.9;
                margin-bottom: 5px;
            }}
            
            .header-info {{
                font-size: 0.9em;
                opacity: 0.7;
            }}
            
            /* KPIs */
            .kpis {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
                gap: 20px;
                margin: 20px 0;
            }}
            
            .kpi-card {{
                background-color: white;
                border-radius: var(--border-radius);
                padding: 20px;
                text-align: center;
                box-shadow: var(--shadow-soft);
                transition: transform var(--transition-speed);
            }}
            
            .kpi-card:hover {{
                transform: translateY(-5px);
                box-shadow: var(--shadow-medium);
            }}
            
            .kpi-title {{
                color: var(--color-gray);
                font-size: 0.9em;
                text-transform: uppercase;
                letter-spacing: 1px;
                margin-bottom: 10px;
            }}
            
            .kpi-value {{
                font-size: 2.5em;
                font-weight: bold;
                margin-bottom: 5px;
            }}
            
            .kpi-subtitle {{
                color: var(--color-gray);
                font-size: 0.85em;
            }}
            
            .nivel-bajo {{
                color: var(--color-success);
            }}
            
            .nivel-medio {{
                color: var(--color-warning);
            }}
            
            .nivel-alto {{
                color: var(--color-danger);
            }}
            
            /* Secciones */
            section {{
                margin: 30px 0;
            }}
            
            h2 {{
                font-size: 1.8em;
                color: var(--color-secondary);
                margin-bottom: 20px;
                padding-bottom: 10px;
                border-bottom: 2px solid var(--color-light);
            }}
            
            .dashboard-row {{
                display: flex;
                flex-wrap: wrap;
                gap: 20px;
                margin-bottom: 30px;
            }}
            
            .card {{
                background-color: white;
                border-radius: var(--border-radius);
                box-shadow: var(--shadow-soft);
                overflow: hidden;
                transition: transform var(--transition-speed), box-shadow var(--transition-speed);
                display: flex;
                flex-direction: column;
            }}
            
            .card:hover {{
                transform: translateY(-5px);
                box-shadow: var(--shadow-medium);
            }}
            
            .card-header {{
                padding: 15px 20px;
                background-color: var(--color-light);
                border-bottom: 1px solid #ddd;
                display: flex;
                justify-content: space-between;
                align-items: center;
            }}
            
            .card-title {{
                font-size: 1.2em;
                font-weight: 600;
                color: var(--color-secondary);
                margin: 0;
            }}
            
            .card-subtitle {{
                color: var(--color-gray);
                font-size: 0.9em;
            }}
            
            .card-body {{
                padding: 20px;
                flex-grow: 1;
                display: flex;
                flex-direction: column;
                justify-content: center;
                align-items: center;
            }}
            
            .wide-card {{
                flex: 2;
            }}
            
            .narrow-card {{
                flex: 1;
                min-width: 300px;
            }}
            
            .img-container {{
                width: 100%;
                text-align: center;
            }}
            
            .img-container img {{
                max-width: 100%;
                height: auto;
            }}
            
            /* Tabla de municipios */
            .tabla-top-municipios {{
                width: 100%;
                border-collapse: collapse;
                margin-top: 10px;
            }}
            
            .tabla-top-municipios th,
            .tabla-top-municipios td {{
                padding: 12px 15px;
                text-align: left;
                border-bottom: 1px solid #ddd;
            }}
            
            .tabla-top-municipios th {{
                background-color: var(--color-light);
                color: var(--color-secondary);
                font-weight: 600;
            }}
            
            .tabla-top-municipios tr:hover {{
                background-color: #f9f9f9;
            }}
            
            .indicador-nivel {{
                width: 15px;
                height: 15px;
                border-radius: 50%;
                display: inline-block;
            }}
            
            .indicador-nivel.bajo {{
                background-color: var(--color-success);
            }}
            
            .indicador-nivel.medio {{
                background-color: var(--color-warning);
            }}
            
            .indicador-nivel.alto {{
                background-color: var(--color-danger);
            }}
            
            .bajo {{
                color: var(--color-success);
            }}
            
            .medio {{
                color: var(--color-warning);
            }}
            
            .alto {{
                color: var(--color-danger);
            }}
            
            /* Footer */
            footer {{
                background-color: var(--color-secondary);
                color: white;
                padding: 30px 0;
                margin-top: 50px;
                text-align: center;
            }}
            
            .footer-content {{
                opacity: 0.8;
            }}
            
            /* Responsive */
            @media (max-width: 768px) {{
                .dashboard-row {{
                    flex-direction: column;
                }}
                
                .kpis {{
                    grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
                }}
                
                .card {{
                    width: 100%;
                }}
                
                h1 {{
                    font-size: 2em;
                }}
            }}
        </style>
    </head>
    <body>
        <header>
            <div class="container header-content">
                <h1>Dashboard de Percepción de Seguridad</h1>
                <div class="header-subtitle">Estado de Yucatán</div>
                <div class="header-info">Periodo: {rango_periodos} | Fecha: {fecha_generacion}</div>
            </div>
        </header>
        
        <div class="container">
            <!-- KPIs principales -->
            {kpis_html}
            
            <!-- Gráfico de tendencia estatal -->
            <section>
                <h2>Evolución de la Percepción de Inseguridad</h2>
                <div class="card wide-card">
                    <div class="card-header">
                        <div class="card-title">Tendencia histórica estatal</div>
                        <div class="card-subtitle">Yucatán {rango_periodos}</div>
                    </div>
                    <div class="card-body">
                        <div class="img-container">
                            <img src="data:image/png;base64,{grafico_tendencia}" alt="Gráfico de tendencia estatal">
                        </div>
                    </div>
                </div>
            </section>
            
            <!-- Comparativas municipales -->
            <section>
                <h2>Análisis Municipal</h2>
                <div class="dashboard-row">
                    <div class="card wide-card">
                        <div class="card-header">
                            <div class="card-title">Comparativa Municipal</div>
                            <div class="card-subtitle">Último periodo: {ultimo_periodo}</div>
                        </div>
                        <div class="card-body">
                            <div class="img-container">
                                <img src="data:image/png;base64,{comparativa_municipal}" alt="Comparativa de municipios">
                            </div>
                        </div>
                    </div>
                    
                    <div class="card narrow-card">
                        <div class="card-header">
                            <div class="card-title">Top Municipios</div>
                            <div class="card-subtitle">Mayor percepción de inseguridad</div>
                        </div>
                        <div class="card-body">
                            {tabla_top_municipios}
                        </div>
                    </div>
                </div>
                
                <!-- Mapa de calor -->
                <div class="card">
                    <div class="card-header">
                        <div class="card-title">Mapa de Calor por Municipio y Periodo</div>
                        <div class="card-subtitle">Municipios principales</div>
                    </div>
                    <div class="card-body">
                        <div class="img-container">
                            <img src="data:image/png;base64,{mapa_calor}" alt="Mapa de calor municipal">
                        </div>
                    </div>
                </div>
            </section>
            
            <!-- Metodología -->
            <section>
                <h2>Metodología</h2>
                <div class="card">
                    <div class="card-header">
                        <div class="card-title">Notas Metodológicas</div>
                    </div>
                    <div class="card-body" style="align-items: flex-start;">
                        <p>Este reporte se basa en datos de la <strong>Encuesta Nacional de Seguridad Pública Urbana (ENSU)</strong> 
                        del INEGI, con las siguientes consideraciones metodológicas:</p>
                        
                        <ul style="margin: 15px 0 15px 20px;">
                            <li>La pregunta analizada es "<strong>BP1_1: ¿Se siente seguro(a) en su municipio?</strong>", 
                            donde las respuestas se codifican como: 1=Seguro, 2=Inseguro, 9=No responde.</li>
                            
                            <li>Los porcentajes se calculan sobre el total de respuestas válidas para cada 
                            municipio y periodo.</li>
                            
                            <li>Los promedios estatales están ponderados por el número de registros 
                            disponibles en cada municipio.</li>
                            
                            <li>Niveles de percepción: <span class="bajo">Verde</span>: Bajo (&lt;30%), 
                            <span class="medio">Amarillo</span>: Medio (30-60%), 
                            <span class="alto">Rojo</span>: Alto (&gt;60%).</li>
                        </ul>
                    </div>
                </div>
            </section>
        </div>
        
        <footer>
            <div class="container">
                <div class="footer-content">
                    <p>Fuente de datos: Instituto Nacional de Estadística y Geografía (INEGI) - 
                    Encuesta Nacional de Seguridad Pública Urbana (ENSU)</p>
                    <p>Generado: {fecha_generacion}</p>
                </div>
            </div>
        </footer>
    </body>
    </html>
    """
    
    return html


def main():
    """Función principal del script."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Generador de dashboard profesional para análisis de percepción de seguridad."
    )
    parser.add_argument(
        "--estado", 
        default="YUCATAN",
        help="Estado a filtrar (default: YUCATAN)"
    )
    parser.add_argument(
        "--dir_procesados", 
        default=".\\data\\yucatan-inseguridad",
        help="Directorio con archivos procesados (default: .\\data\\yucatan-inseguridad)"
    )
    parser.add_argument(
        "--dir_reportes", 
        default=".\\data\\yucatan-inseguridad\\reportes",
        help="Directorio para guardar reportes (default: .\\data\\yucatan-inseguridad\\reportes)"
    )
    parser.add_argument(
        "--log", 
        default=".\\data\\yucatan-inseguridad\\logs\\dashboard.log",
        help="Archivo de log (default: .\\data\\yucatan-inseguridad\\logs\\dashboard.log)"
    )
    
    args = parser.parse_args()
    
    try:
        # Configurar logging
        logger = setup_logging(args.log)
        logger.info(f"Iniciando generación de dashboard para el estado: {args.estado}")
        
        # Cargar datos procesados
        df = cargar_datos_procesados(args.dir_procesados, args.estado, logger)
        
        # Cargar resumen de procesamiento
        resumen = {}
        try:
            with open(os.path.join(args.dir_procesados, "logs", "resumen_procesamiento.json"), 'r', encoding='utf-8') as f:
                resumen = json.load(f)
        except Exception as e:
            logger.warning(f"No se pudo cargar el resumen: {str(e)}")
        
        # Calcular promedio estatal
        df_estatal, promedio_estatal = calcular_promedio_estatal(df, logger)
        
        # Generar HTML
        logger.info("Generando dashboard HTML")
        html = generar_dashboard_html(df, df_estatal, resumen, logger)
        
        # Crear directorio de reportes si no existe
        os.makedirs(args.dir_reportes, exist_ok=True)
        
        # Guardar reporte HTML
        reporte_path = os.path.join(args.dir_reportes, "dashboard_yucatan.html")
        with open(reporte_path, "w", encoding="utf-8") as f:
            f.write(html)
        
        logger.info(f"Dashboard HTML guardado en: {reporte_path}")
        print(f"\nDashboard profesional generado exitosamente en: {reporte_path}")
        
    except Exception as e:
        if 'logger' in locals():
            logger.exception(f"Error durante la generación del dashboard: {str(e)}")
        else:
            print(f"Error durante la generación del dashboard: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
