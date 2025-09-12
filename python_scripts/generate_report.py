
import pandas as pd
import matplotlib.pyplot as plt
import os

def generate_markdown_report():
    """
    Generates a markdown report with a static plot of the insecurity trend.
    """
    # Define paths relative to the script location
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_path = os.path.join(script_dir, 'yucatan_consolidated_data.csv')
    image_path = os.path.join(script_dir, 'yucatan_insecurity_trend.png')
    report_path = os.path.join(script_dir, 'reporte_yucatan.md')

    # --- 1. Read Data ---
    try:
        df = pd.read_csv(data_path)
        df['Periodo'] = 'A' + df['AÑO'].astype(str) + '-T' + df['TRIMESTRE'].astype(str)
    except FileNotFoundError:
        print(f"ERROR: Consolidated data file not found at {data_path}")
        return

    # --- 2. Calculate Statewide Trend (Weighted Average) ---
    def get_state_trend(group):
        total_respondents = group['TOTAL_REGISTROS'].sum()
        total_insecure = group['TOTAL_INSEGUROS'].sum()
        return (total_insecure / total_respondents) * 100 if total_respondents > 0 else 0

    state_trend = df.groupby(['AÑO', 'TRIMESTRE']).apply(get_state_trend).reset_index(name='PCT_INSEGUROS_ESTATAL')
    state_trend['Periodo'] = 'A' + state_trend['AÑO'].astype(str) + '-T' + state_trend['TRIMESTRE'].astype(str)
    state_trend.sort_values(by=['AÑO', 'TRIMESTRE'], inplace=True)

    # --- 3. Generate Plot ---
    plt.style.use('seaborn-v0_8-whitegrid')
    fig, ax = plt.subplots(figsize=(12, 7))
    
    ax.plot(state_trend['Periodo'], state_trend['PCT_INSEGUROS_ESTATAL'], marker='o', linestyle='-', color='#0d6efd')
    
    ax.set_title('Tendencia de Percepción de Inseguridad en Yucatán', fontsize=16, weight='bold')
    ax.set_ylabel('Porcentaje de Percepción de Inseguridad (%)')
    ax.set_xlabel('Periodo (Año-Trimestre)')
    ax.tick_params(axis='x', rotation=45)
    ax.grid(True, which='both', linestyle='--', linewidth=0.5)
    fig.tight_layout() # Adjust layout to make room for rotated x-axis labels
    
    plt.savefig(image_path)
    print(f"SUCCESS: Static plot saved to {image_path}")

    # --- 4. Generate Markdown Report ---
    md_content = []
    first_period = state_trend['Periodo'].iloc[0]
    last_period = state_trend['Periodo'].iloc[-1]
    generation_date = pd.Timestamp.now().strftime('%d de %B de %Y')

    md_content.append("# Reporte de Percepción de Inseguridad - Yucatán")
    md_content.append("---" * 20)
    md_content.append(f"**Periodo de Análisis:** De {first_period} a {last_period}.")
    md_content.append(f"**Fecha de Generación:** {generation_date}.")
    
    md_content.append("## Resumen Ejecutivo")
    md_content.append("El presente reporte analiza la evolución de la percepción de inseguridad en el estado de Yucatán, consolidando los datos de las encuestas trimestrales. A continuación se presenta el gráfico de tendencia estatal y la tabla de datos completa.")

    md_content.append("## Tendencia de Inseguridad Estatal")
    md_content.append(f"![Tendencia de Inseguridad en Yucatán]({os.path.basename(image_path)})")

    md_content.append("## Tabla de Datos Detallada")
    md_content.append(df.to_markdown(index=False))

    md_content.append("## Conclusiones")
    md_content.append("_(Espacio reservado para el análisis y las conclusiones derivadas de los datos presentados.)_")

    # Write the markdown file
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write("\n\n".join(md_content))
    
    print(f"SUCCESS: Markdown report saved to {report_path}")

if __name__ == '__main__':
    generate_markdown_report()
