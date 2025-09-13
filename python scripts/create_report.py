import pandas as pd
import matplotlib.pyplot as plt
import os

# --- Configuration ---
DATA_FILE = '/home/danielgomez/Documents/data_mining/data-mining/reporte/consolidado_yucatan.csv'
OUTPUT_DIR = '/home/danielgomez/Documents/data_mining/data-mining/reporte'
IMAGE_FILE = os.path.join(OUTPUT_DIR, 'tendencia_inseguridad.png')
REPORT_FILE = os.path.join(OUTPUT_DIR, 'reporte_final.md')

def create_trend_plot(df):
    """Creates and saves a trend plot of insecurity perception."""
    print("Generando datos para el gráfico de tendencia...")
    
    # Ensure AÑO and TRIMESTRE are treated as strings for sorting and combining
    df['AÑO'] = df['AÑO'].astype(str)
    df['TRIMESTRE'] = df['TRIMESTRE'].astype(str)
    
    # Create a combined period column for plotting
    df['PERIODO'] = df['AÑO'] + '-T' + df['TRIMESTRE']
    
    # Aggregate data at the state level to get a single trend line
    state_level_trend = df.groupby('PERIODO').agg(
        SUM_INSEGUROS=('TOTAL_INSEGUROS', 'sum'),
        SUM_REGISTROS=('TOTAL_REGISTROS', 'sum')
    ).reset_index()
    
    state_level_trend['PCT_INSEGUROS_ESTATAL'] = (state_level_trend['SUM_INSEGUROS'] / state_level_trend['SUM_REGISTROS']) * 100
    
    # Sort by period to ensure the line plot is correct
    state_level_trend = state_level_trend.sort_values('PERIODO')

    print("Creando gráfico...")
    plt.style.use('seaborn-v0_8-whitegrid')
    fig, ax = plt.subplots(figsize=(12, 7))
    
    ax.plot(state_level_trend['PERIODO'], state_level_trend['PCT_INSEGUROS_ESTATAL'], marker='o', linestyle='-', color='#c0392b')
    
    # Formatting
    ax.set_title('Tendencia de Percepción de Inseguridad en Yucatán', fontsize=16, weight='bold')
    ax.set_ylabel('Porcentaje de Población que se Siente Insegura (%)', fontsize=12)
    ax.set_xlabel('Periodo (Año-Trimestre)', fontsize=12)
    plt.xticks(rotation=45, ha='right')
    ax.grid(True, which='both', linestyle='--', linewidth=0.5)
    fig.tight_layout() # Adjust layout to make room for rotated x-axis labels
    
    # Save the plot
    plt.savefig(IMAGE_FILE)
    print(f"Gráfico guardado en: {IMAGE_FILE}")

def create_markdown_report(df):
    """Creates the final Markdown report."""
    print("Creando informe en formato Markdown...")
    
    with open(REPORT_FILE, 'w', encoding='utf-8') as f:
        # 1. Header
        f.write("# Reporte de Percepción de Inseguridad en Yucatán\n\n")
        f.write("Este reporte presenta un análisis de la percepción de seguridad en los municipios de Yucatán, basado en los datos de la Encuesta Nacional de Seguridad Pública Urbana (ENSU).\n\n")
        
        # 2. Plot
        f.write("## Tendencia General de Inseguridad\n\n")
        f.write("El siguiente gráfico muestra la evolución del porcentaje de la población que se siente insegura en el estado a lo largo de diferentes trimestres.\n\n")
        f.write(f"![Tendencia de Inseguridad]({os.path.basename(IMAGE_FILE)})\n\n")
        
        # 3. Summary per Quarter (Optional - for this version we will skip to the full table)
        
        # 4. Data Table
        f.write("## Tabla de Datos Consolidados\n\n")
        f.write("La siguiente tabla contiene los datos agregados por municipio para cada periodo analizado.\n\n")
        f.write(df.to_markdown(index=False))
        f.write("\n\n")
        
        # 5. Conclusions
        f.write("## Conclusiones\n\n")
        f.write("[Análisis y conclusiones a ser completados por el usuario.]\n")
        
    print(f"Informe guardado en: {REPORT_FILE}")

def main():
    """Main function to generate the report."""
    print("===== Iniciando la Creación del Reporte Final =====")
    try:
        df = pd.read_csv(DATA_FILE)
    except FileNotFoundError:
        print(f"Error: No se encontró el archivo de datos consolidado en {DATA_FILE}. Ejecuta primero 'process_yucatan_data.py'.")
        return

    create_trend_plot(df.copy()) # Pass a copy to avoid modifying the original df
    create_markdown_report(df)
    
    print("===== Creación del Reporte Finalizada =====")

if __name__ == '__main__':
    main()
