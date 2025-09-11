
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

def generate_report_assets():
    # Rutas de archivos
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_path = os.path.join(script_dir, '..', 'reports', 'dataset_yucatan.csv')
    reports_dir = os.path.join(script_dir, '..', 'reports')
    
    # Cargar datos
    df = pd.read_csv(data_path)
    
    # Limpieza de datos
    df['NOM_MUN'] = df['NOM_MUN'].str.strip().str.capitalize()
    df['PERIODO'] = df['AÑO'].astype(str) + '-Q' + df['TRIMESTRE'].astype(str)
    df['PCT_INSEGUROS'] = df['PCT_INSEGUROS'] * 100

    # Paleta de colores para las gráficas
    color_palette = sns.color_palette("Reds_d", n_colors=len(df['NOM_MUN'].unique()))

    # 1. Gráfica de comparación por municipio
    plt.style.use('seaborn-v0_8-whitegrid')
    fig, ax = plt.subplots(figsize=(12, 7))
    
    sns.lineplot(data=df, x='PERIODO', y='PCT_INSEGUROS', hue='NOM_MUN', ax=ax, palette=color_palette, marker='o')
    
    ax.set_title('Percepción de Inseguridad por Municipio en Yucatán', fontsize=16, color='#8B0000')
    ax.set_xlabel('Periodo', fontsize=12)
    ax.set_ylabel('Porcentaje de Percepción de Inseguridad (%)', fontsize=12)
    ax.tick_params(axis='x', labelrotation=90)
    ax.legend(title='Municipio')
    
    plt.tight_layout()
    insecurity_by_municipality_path = os.path.join(reports_dir, 'insecurity_by_municipality.png')
    plt.savefig(insecurity_by_municipality_path)
    plt.close(fig)

    # 2. Tabla de resumen
    summary_table = df.groupby('NOM_MUN')['PCT_INSEGUROS'].agg(['mean', 'min', 'max', 'std']).reset_index()
    summary_table = summary_table.rename(columns={
        'NOM_MUN': 'Municipio',
        'mean': 'Media (%)',
        'min': 'Mínimo (%)',
        'max': 'Máximo (%)',
        'std': 'Desv. Estándar'
    })
    
    # Convertir a HTML
    summary_html = summary_table.to_html(index=False, classes='summary-table', border=0, float_format='%.2f')
    
    # Guardar la tabla HTML en un archivo para ser leída después
    summary_html_path = os.path.join(reports_dir, 'summary_table.html')
    with open(summary_html_path, 'w') as f:
        f.write(summary_html)

if __name__ == '__main__':
    generate_report_assets()
