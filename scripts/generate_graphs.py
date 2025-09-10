import os
import glob
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import logging

# --- Configuración ---
INPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "yucatan-inseguridad")
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "images")
LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs", "graph_generation.log")

# --- Configuración de Logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, mode='w', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

def load_and_prepare_data():
    """Carga y concatena todos los CSV, y prepara los datos para graficar."""
    csv_files = glob.glob(os.path.join(INPUT_DIR, "*.csv"))
    if not csv_files:
        logging.warning(f"No se encontraron archivos CSV en: {INPUT_DIR}")
        return None

    logging.info(f"Se encontraron {len(csv_files)} archivos CSV para procesar.")
    df = pd.concat((pd.read_csv(f) for f in csv_files), ignore_index=True)

    # Preparación de datos
    df['PERIODO'] = df['AÑO'].astype(str) + '-Q' + df['TRIMESTRE'].astype(str)
    df.sort_values(by=['AÑO', 'TRIMESTRE'], inplace=True)
    
    logging.info(f"Datos cargados. Total de registros: {len(df)}. Períodos de {df['PERIODO'].min()} a {df['PERIODO'].max()}.")
    return df

def generate_state_evolution_graph(df):
    """Gráfico 1: Evolución de la Percepción de Seguridad en Yucatán (Promedio Ponderado)."""
    logging.info("Generando Gráfico 1: Evolución estatal...")
    
    # CORRECCIÓN: Se añade include_groups=False para evitar el FutureWarning de pandas.
    weighted_avg = df.groupby('PERIODO').apply(
        lambda x: pd.Series({
            'PCT_SEGUROS_PONDERADO': (x['PCT_SEGUROS'] * x['TOTAL_REGISTROS']).sum() / x['TOTAL_REGISTROS'].sum(),
            'PCT_INSEGUROS_PONDERADO': (x['PCT_INSEGUROS'] * x['TOTAL_REGISTROS']).sum() / x['TOTAL_REGISTROS'].sum()
        }), include_groups=False
    ).reset_index()
    weighted_avg.sort_values(by='PERIODO', inplace=True)

    plt.style.use('seaborn-v0_8-whitegrid')
    fig, ax = plt.subplots(figsize=(14, 7))
    
    ax.plot(weighted_avg['PERIODO'], weighted_avg['PCT_SEGUROS_PONDERADO'], marker='o', linestyle='-', color='g', label='Seguridad')
    ax.plot(weighted_avg['PERIODO'], weighted_avg['PCT_INSEGUROS_PONDERADO'], marker='o', linestyle='-', color='r', label='Inseguridad')
    
    ax.set_title('Evolución de la Percepción de Seguridad en Yucatán (Promedio Ponderado)', fontsize=16)
    ax.set_xlabel('Período (Año-Trimestre)')
    ax.set_ylabel('Porcentaje (%)')
    ax.legend()
    ax.tick_params(axis='x', rotation=45)
    plt.tight_layout()
    
    output_path = os.path.join(OUTPUT_DIR, "01_evolucion_seguridad_yucatan.png")
    plt.savefig(output_path)
    plt.close(fig)
    logging.info(f"Gráfico 1 guardado en: {output_path}")

def generate_municipality_comparison_graph(df):
    """Gráfico 2: Comparativa de Inseguridad por Municipio (Trimestre más Reciente)."""
    logging.info("Generando Gráfico 2: Comparativa municipal reciente...")
    
    latest_period = df['PERIODO'].max()
    df_latest = df[df['PERIODO'] == latest_period].sort_values(by='PCT_INSEGUROS', ascending=False)

    plt.style.use('seaborn-v0_8-whitegrid')
    fig, ax = plt.subplots(figsize=(14, 8))
    
    # CORRECCIÓN: Se añade hue y legend=False para evitar el FutureWarning de seaborn.
    sns.barplot(x='NOM_MUN', y='PCT_INSEGUROS', data=df_latest, ax=ax, palette='viridis', hue='NOM_MUN', legend=False)
    
    ax.set_title(f'Percepción de Inseguridad por Municipio ({latest_period})', fontsize=16)
    ax.set_xlabel('Municipio')
    ax.set_ylabel('Porcentaje de Inseguridad (%)')
    
    # CORRECCIÓN: Se usa setp para rotar las etiquetas, que es un método más robusto.
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right", rotation_mode="anchor")
    
    plt.tight_layout()
    
    output_path = os.path.join(OUTPUT_DIR, "02_comparativa_municipios_reciente.png")
    plt.savefig(output_path)
    plt.close(fig)
    logging.info(f"Gráfico 2 guardado en: {output_path}")

def generate_major_municipalities_evolution_graph(df):
    """Gráfico 3: Evolución por Municipios Principales."""
    logging.info("Generando Gráfico 3: Evolución de municipios principales...")
    
    top_municipalities = df.groupby('NOM_MUN')['TOTAL_REGISTROS'].sum().nlargest(4).index
    df_top = df[df['NOM_MUN'].isin(top_municipalities)]

    plt.style.use('seaborn-v0_8-whitegrid')
    fig, ax = plt.subplots(figsize=(14, 7))
    
    sns.lineplot(data=df_top, x='PERIODO', y='PCT_INSEGUROS', hue='NOM_MUN', marker='o', ax=ax)
    
    ax.set_title('Evolución de la Percepción de Inseguridad en Municipios Principales', fontsize=16)
    ax.set_xlabel('Período (Año-Trimestre)')
    ax.set_ylabel('Porcentaje de Inseguridad (%)')
    ax.legend(title='Municipio')
    ax.tick_params(axis='x', rotation=45)
    plt.tight_layout()
    
    output_path = os.path.join(OUTPUT_DIR, "03_evolucion_municipios_principales.png")
    plt.savefig(output_path)
    plt.close(fig)
    logging.info(f"Gráfico 3 guardado en: {output_path}")

def generate_distribution_graph(df):
    """Gráfico 4: Distribución de la Percepción de Seguridad entre Municipios."""
    logging.info("Generando Gráfico 4: Distribución de percepción...")
    
    latest_period = df['PERIODO'].max()
    df_latest = df[df['PERIODO'] == latest_period]

    plt.style.use('seaborn-v0_8-whitegrid')
    fig, ax = plt.subplots(figsize=(12, 6))
    
    sns.boxplot(x=df_latest['PCT_SEGUROS'], ax=ax)
    sns.stripplot(x=df_latest['PCT_SEGUROS'], ax=ax, color=".25", size=5)

    ax.set_title(f'Distribución del Porcentaje de Seguridad entre Municipios ({latest_period})', fontsize=16)
    ax.set_xlabel('Porcentaje de Seguridad (%)')
    plt.tight_layout()
    
    output_path = os.path.join(OUTPUT_DIR, "04_distribucion_seguridad_reciente.png")
    plt.savefig(output_path)
    plt.close(fig)
    logging.info(f"Gráfico 4 guardado en: {output_path}")

def main():
    """Función principal para orquestar la generación de gráficos."""
    logging.info("--- Iniciando script de generación de gráficos ---")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    logging.info(f"Directorio de salida asegurado: {OUTPUT_DIR}")

    df = load_and_prepare_data()

    if df is not None and not df.empty:
        generate_state_evolution_graph(df)
        generate_municipality_comparison_graph(df)
        generate_major_municipalities_evolution_graph(df)
        generate_distribution_graph(df)
        logging.info("--- Finalizado script de generación de gráficos ---")
    else:
        logging.error("No hay datos para generar gráficos. Terminando script.")

if __name__ == "__main__":
    main()