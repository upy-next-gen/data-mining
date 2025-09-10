
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import glob
import os

def clean_data(df):
    """Limpia y prepara el DataFrame."""
    df['NOM_MUN'] = df['NOM_MUN'].str.strip().str.upper()
    df['NOM_ENT'] = df['NOM_ENT'].str.strip().str.upper()
    df.replace('', np.nan, inplace=True)
    df['NOM_MUN'].fillna('ESTATAL', inplace=True)
    df['PCT_INSEGUROS'] = pd.to_numeric(df['PCT_INSEGUROS'], errors='coerce')
    df['TOTAL_REGISTROS'] = pd.to_numeric(df['TOTAL_REGISTROS'], errors='coerce')
    df.dropna(subset=['PCT_INSEGUROS', 'TOTAL_REGISTROS'], inplace=True)
    return df

def weighted_avg(group):
    """Calcula el promedio ponderado para la percepción de inseguridad."""
    return (group['PCT_INSEGUROS'] * group['TOTAL_REGISTROS']).sum() / group['TOTAL_REGISTROS'].sum()

def main():
    """Función principal para generar gráficos y datos para el reporte."""
    # Cargar y consolidar datos
    path = 'data/processed/'
    all_files = glob.glob(os.path.join(path, "processed_ensu_*.csv"))
    df = pd.concat((pd.read_csv(f) for f in all_files), ignore_index=True)

    df = clean_data(df)
    df['PERIODO'] = df['AÑO'].astype(str) + '-T' + df['TRIMESTRE'].astype(str)
    df.sort_values(['AÑO', 'TRIMESTRE'], inplace=True)

    # --- 1. Evolución de la Percepción de Inseguridad ---
    evolucion = df.groupby(['AÑO', 'TRIMESTRE']).apply(weighted_avg).reset_index(name='PCT_INSEGUROS_PROM')
    evolucion['PERIODO'] = evolucion['AÑO'].astype(str) + '-T' + evolucion['TRIMESTRE'].astype(str)
    
    plt.style.use('seaborn-v0_8-whitegrid')
    fig, ax = plt.subplots(figsize=(12, 7))
    ax.plot(evolucion['PERIODO'], evolucion['PCT_INSEGUROS_PROM'], marker='o', linestyle='-', color='b')
    ax.set_title('Evolución de la Percepción de Inseguridad en Yucatán (2015-2025)', fontsize=16)
    ax.set_ylabel('Percepción de Inseguridad (%)')
    ax.set_xlabel('Periodo')
    ax.tick_params(axis='x', rotation=45)
    ax.yaxis.set_major_formatter(mticker.PercentFormatter())
    plt.tight_layout()
    plt.savefig('reports/images/evolucion_inseguridad.png')
    plt.close()
    print("--- EVOLUCION_DATA ---")
    print(evolucion[['PERIODO', 'PCT_INSEGUROS_PROM']].to_csv(index=False))

    # --- 2. Comparativa de Inseguridad por Municipio (Último Periodo) ---
    ultimo_año = df['AÑO'].max()
    ultimo_trimestre = df[df['AÑO'] == ultimo_año]['TRIMESTRE'].max()
    comparativa_df = df[(df['AÑO'] == ultimo_año) & (df['TRIMESTRE'] == ultimo_trimestre) & (df['NOM_MUN'] != 'ESTATAL')]
    comparativa_df = comparativa_df.sort_values('PCT_INSEGUROS', ascending=False)

    if not comparativa_df.empty:
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.bar(comparativa_df['NOM_MUN'], comparativa_df['PCT_INSEGUROS'], color='skyblue')
        ax.set_title(f'Comparativa de Inseguridad por Municipio ({ultimo_año}-T{ultimo_trimestre})', fontsize=16)
        ax.set_ylabel('Percepción de Inseguridad (%)')
        ax.set_xlabel('Municipio')
        ax.yaxis.set_major_formatter(mticker.PercentFormatter())
        plt.tight_layout()
        plt.savefig('reports/images/comparativa_municipios.png')
        plt.close()
        print("\n--- COMPARATIVA_DATA ---")
        print(comparativa_df[['NOM_MUN', 'PCT_INSEGUROS']].to_csv(index=False))

    # --- 3. Periodos Críticos de Inseguridad ---
    periodos_criticos = evolucion.sort_values('PCT_INSEGUROS_PROM', ascending=False).head(5)
    
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.bar(periodos_criticos['PERIODO'], periodos_criticos['PCT_INSEGUROS_PROM'], color='salmon')
    ax.set_title('Top 5 Periodos Críticos de Inseguridad', fontsize=16)
    ax.set_ylabel('Percepción de Inseguridad (%)')
    ax.set_xlabel('Periodo')
    ax.yaxis.set_major_formatter(mticker.PercentFormatter())
    plt.tight_layout()
    plt.savefig('reports/images/periodos_criticos.png')
    plt.close()
    print("\n--- CRITICOS_DATA ---")
    print(periodos_criticos[['PERIODO', 'PCT_INSEGUROS_PROM']].to_csv(index=False))

if __name__ == '__main__':
    main()
