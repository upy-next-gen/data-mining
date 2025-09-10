
import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

def generar_reporte_analitico():
    """
    Genera un reporte analítico en HTML a partir de los datos de inseguridad,
    incluyendo estadísticas descriptivas y visualizaciones.
    """
    input_dir = os.path.join('data', 'yucatan-inseguridad')
    output_html = 'reporte_analitico.html'
    plot_file = 'inseguridad_tendencia.png'
    
    if not os.path.exists(input_dir):
        print(f"Error: El directorio '{input_dir}' no fue encontrado.")
        return

    csv_files = [f for f in os.listdir(input_dir) if f.endswith('.csv')]
    if not csv_files:
        print(f"No se encontraron archivos CSV en '{input_dir}'.")
        return

    df_list = []
    for file in csv_files:
        file_path = os.path.join(input_dir, file)
        try:
            df = pd.read_csv(file_path)
            # Asegurarse que las columnas existen antes de procesar
            if 'AÑO' in df.columns and 'TRIMESTRE' in df.columns:
                df_list.append(df)
            else:
                print(f"Advertencia: El archivo {file} no contiene las columnas 'AÑO' o 'TRIMESTRE' y será ignorado.")
        except Exception as e:
            print(f"Error al leer {file}: {e}")

    if not df_list:
        print("No se pudieron cargar datos para el reporte.")
        return
        
    combined_df = pd.concat(df_list, ignore_index=True)

    # --- Análisis de Datos ---
    # 1. Limpieza y preparación
    combined_df.rename(columns={'AÑO': 'Anio', 'TRIMESTRE': 'Trimestre', 'PCT_INSEGUROS': 'Pct_Inseguros'}, inplace=True)
    combined_df['Fecha'] = pd.to_datetime(combined_df['Anio'].astype(str) + 'Q' + combined_df['Trimestre'].astype(str).str[0])
    
    # 2. Agregación por fecha
    quarterly_insecurity = combined_df.groupby('Fecha')['Pct_Inseguros'].mean().reset_index().sort_values('Fecha')

    # 3. Estadísticas Descriptivas
    desc_stats = quarterly_insecurity['Pct_Inseguros'].describe()
    
    # 4. Puntos Clave
    max_insecurity = quarterly_insecurity.loc[quarterly_insecurity['Pct_Inseguros'].idxmax()]
    min_insecurity = quarterly_insecurity.loc[quarterly_insecurity['Pct_Inseguros'].idxmin()]

    # --- Generación de Gráficos ---
    plt.style.use('seaborn-v0_8-whitegrid')
    fig, ax = plt.subplots(figsize=(12, 7))
    
    ax.plot(quarterly_insecurity['Fecha'], quarterly_insecurity['Pct_Inseguros'], marker='o', linestyle='-', color='firebrick')
    
    # Formato del gráfico
    ax.set_title('Evolución de la Percepción de Inseguridad Promedio (Trimestral)', fontsize=16, weight='bold')
    ax.set_ylabel('Porcentaje de Percepción de Inseguridad (%)', fontsize=12)
    ax.set_xlabel('Fecha (Año-Trimestre)', fontsize=12)
    ax.grid(True, which='both', linestyle='--', linewidth=0.5)
    plt.xticks(rotation=45)
    ax.set_ylim(bottom=0)
    fig.tight_layout()
    
    # Guardar el gráfico
    plt.savefig(plot_file)
    plt.close()

    # --- Generación de HTML ---
    stats_table_html = pd.DataFrame(desc_stats).to_html(classes='table table-bordered', header=False)
    quarterly_table_html = quarterly_insecurity.to_html(classes='table table-striped', index=False)

    html_content = f"""
    <!DOCTYPE html>
    <html lang="es">
    <head>
        <meta charset="UTF-8">
        <title>Reporte Analítico de Inseguridad</title>
        <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
        <style>
            body {{ font-family: sans-serif; }}
            .container {{ max-width: 960px; }}
            .card-header {{ background-color: #f8f9fa; }}
            h1, h2 {{ color: #343a40; }}
        </style>
    </head>
    <body>
        <div class="container my-5">
            <h1 class="text-center mb-4">Reporte Analítico de Percepción de Inseguridad</h1>
            
            <div class="card mb-4">
                <div class="card-header"><h2>Principales Hallazgos</h2></div>
                <div class="card-body">
                    <p>El análisis de los datos trimestrales revela los siguientes puntos clave:</p>
                    <ul>
                        <li><strong>Punto más alto de inseguridad:</strong> Se registró en <strong>{max_insecurity['Fecha'].strftime('%Y-%m')}</strong> con un promedio de <strong>{max_insecurity['Pct_Inseguros']:.2f}%</strong>.</li>
                        <li><strong>Punto más bajo de inseguridad:</strong> Se registró en <strong>{min_insecurity['Fecha'].strftime('%Y-%m')}</strong> con un promedio de <strong>{min_insecurity['Pct_Inseguros']:.2f}%</strong>.</li>
                    </ul>
                </div>
            </div>

            <div class="card mb-4">
                <div class="card-header"><h2>Evolución de la Percepción de Inseguridad</h2></div>
                <div class="card-body">
                    <p>El siguiente gráfico muestra la tendencia del porcentaje promedio de la percepción de inseguridad a lo largo del tiempo.</p>
                    <img src="{plot_file}" class="img-fluid" alt="Gráfico de tendencia de inseguridad">
                </div>
            </div>

            <div class="card mb-4">
                <div class="card-header"><h2>Estadísticas Descriptivas</h2></div>
                <div class="card-body">
                    <p>La tabla a continuación resume las estadísticas para el porcentaje de inseguridad promedio trimestral.</p>
                    {stats_table_html}
                </div>
            </div>
            
            <div class="card">
                <div class="card-header"><h2>Datos Agregados por Trimestre</h2></div>
                <div class="card-body">
                    {quarterly_table_html}
                </div>
            </div>
        </div>
    </body>
    </html>
    """

    try:
        with open(output_html, 'w', encoding='utf-8') as f:
            f.write(html_content)
        print(f"Reporte analítico generado exitosamente en '{output_html}'")
    except Exception as e:
        print(f"Error al escribir el archivo HTML: {e}")

if __name__ == '__main__':
    generar_reporte_analitico()
