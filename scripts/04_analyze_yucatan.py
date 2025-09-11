import pandas as pd
import matplotlib.pyplot as plt
import os

def analyze_yucatan_data():
    # Define paths
    csv_path = 'reports/dataset_yucatan.csv'
    plot_path = 'reports/insecurity_trend_yucatan.png'
    html_path = 'reports/analisis_yucatan.html'

    # Read the dataset
    if not os.path.exists(csv_path):
        print(f"Error: {csv_path} not found. Please generate it first.")
        return
    df = pd.read_csv(csv_path)

    # --- Data Preparation ---
    # Create a sortable period column
    df['PERIODO'] = df['AÑO'].astype(str) + '-Q' + df['TRIMESTRE'].astype(str)
    df_sorted = df.sort_values(by=['AÑO', 'TRIMESTRE'])

    # Calculate state-level weighted average for insecurity
    # Numerator: sum(PCT_INSEGUROS * TOTAL_REGISTROS) for each period
    # Denominator: sum(TOTAL_REGISTROS) for each period
    df_sorted['INSEGUROS_PONDERADO'] = df_sorted['PCT_INSEGUROS'] * df_sorted['TOTAL_REGISTROS']
    state_level = df_sorted.groupby('PERIODO').agg(
        SUMA_INSEGUROS_PONDERADO=('INSEGUROS_PONDERADO', 'sum'),
        SUMA_TOTAL_REGISTROS=('TOTAL_REGISTROS', 'sum')
    ).reset_index()

    state_level['PCT_INSEGURIDAD_ESTATAL'] = state_level['SUMA_INSEGUROS_PONDERADO'] / state_level['SUMA_TOTAL_REGISTROS']
    # Re-sort based on the period string
    state_level = state_level.sort_values(by='PERIODO')

    # --- Visualization ---
    plt.style.use('seaborn-v0_8-whitegrid')
    fig, ax = plt.subplots(figsize=(12, 7))

    ax.plot(state_level['PERIODO'], state_level['PCT_INSEGURIDAD_ESTATAL'], marker='o', linestyle='-', color='#c0392b')
    ax.set_title('Evolución de la Percepción de Inseguridad en Yucatán', fontsize=16, weight='bold')
    ax.set_ylabel('Porcentaje de Percepción de Inseguridad (Promedio Ponderado)', fontsize=12)
    ax.set_xlabel('Periodo (Año-Trimestre)', fontsize=12)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda y, _: f'{y:.0%}')) # Format y-axis as percentage
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout() # Adjust layout to make room for rotated x-axis labels
    
    # Save the plot
    plt.savefig(plot_path)
    print(f"Plot saved to {plot_path}")

    # --- HTML Report Generation ---
    # Determine the trend for the conclusion
    first_val = state_level['PCT_INSEGURIDAD_ESTATAL'].iloc[0]
    last_val = state_level['PCT_INSEGURIDAD_ESTATAL'].iloc[-1]
    peak_val = state_level['PCT_INSEGURIDAD_ESTATAL'].max()
    peak_period = state_level.loc[state_level['PCT_INSEGURIDAD_ESTATAL'].idxmax(), 'PERIODO']

    conclusion_text = f"""
    El análisis de los datos desde {state_level['PERIODO'].iloc[0]} hasta {state_level['PERIODO'].iloc[-1]} revela varias tendencias clave en la percepción de inseguridad en Yucatán.</p>
    <p>La percepción de inseguridad comenzó en un <strong>{first_val:.1%}</strong> y terminó en <strong>{last_val:.1%}</strong>. 
    El punto más alto de inseguridad percibida se registró en el periodo <strong>{peak_period}</strong>, alcanzando un <strong>{peak_val:.1%}</strong>.
    """
    if last_val > first_val:
        conclusion_text += "En general, la gráfica muestra una <strong>tendencia al alza</strong> en la percepción de inseguridad a lo largo del tiempo, a pesar de fluctuaciones trimestrales."
    else:
        conclusion_text += "En general, la gráfica muestra una <strong>tendencia a la baja</strong> en la percepción de inseguridad a lo largo del tiempo, a pesar de fluctuaciones trimestrales."

    html_content = f"""
    <!DOCTYPE html>
    <html lang="es">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Análisis de Percepción de Inseguridad en Yucatán</title>
        <style>
            body {{ font-family: sans-serif; margin: 40px; background-color: #f4f4f9; color: #333; }}
            .container {{ max-width: 800px; margin: auto; background: #fff; padding: 20px; border-radius: 8px; box-shadow: 0 0 10px rgba(0,0,0,0.1); }}
            h1 {{ color: #2c3e50; }}
            img {{ max-width: 100%; height: auto; border-radius: 8px; margin-top: 20px; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>Análisis de Percepción de Inseguridad en Yucatán</h1>
            <p>Este reporte presenta la evolución de la percepción de inseguridad en el estado de Yucatán, basada en los datos procesados de la ENSU. La métrica principal es el porcentaje de la población que percibe su entorno como inseguro, ponderado por el número de encuestados en cada municipio para obtener una visión a nivel estatal.</p>
            <img src="insecurity_trend_yucatan.png" alt="Gráfica de la evolución de la inseguridad en Yucatán">
            <h2>Conclusión</h2>
            <p>{conclusion_text}</p>
        </div>
    </body>
    </html>
    """

    with open(html_path, 'w', encoding='utf-8') as f:
        f.write(html_content)
    print(f"HTML report saved to {html_path}")

if __name__ == '__main__':
    analyze_yucatan_data()
