
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

def generate_additional_report_assets():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_path = os.path.join(script_dir, '..', 'reports', 'dataset_yucatan.csv')
    reports_dir = os.path.join(script_dir, '..', 'reports')

    df = pd.read_csv(data_path)
    df['NOM_MUN'] = df['NOM_MUN'].str.strip().str.capitalize()
    df['PERIODO'] = df['AÑO'].astype(str) + '-Q' + df['TRIMESTRE'].astype(str)

    # 1. Gráfico de promedios de percepción por municipio (tipo burbuja/barras)
    avg_perception = df.groupby('NOM_MUN')[['PCT_INSEGUROS', 'PCT_SEGUROS']].mean().reset_index()
    avg_perception['PCT_INSEGUROS'] = avg_perception['PCT_INSEGUROS'] * 100
    avg_perception['PCT_SEGUROS'] = avg_perception['PCT_SEGUROS'] * 100

    plt.style.use('seaborn-v0_8-whitegrid')
    fig, ax = plt.subplots(figsize=(10, 6))
    
    avg_perception.set_index('NOM_MUN').plot(kind='bar', ax=ax, color=['#CD5C5C', '#8B0000'])
    
    ax.set_title('Promedio de Percepción de Seguridad/Inseguridad por Municipio', fontsize=16, color='#8B0000')
    ax.set_xlabel('Municipio', fontsize=12)
    ax.set_ylabel('Porcentaje Promedio (%)', fontsize=12)
    ax.tick_params(axis='x', rotation=45)
    ax.legend(['Inseguro', 'Seguro'], title='Percepción')
    plt.tight_layout()
    avg_perception_plot_path = os.path.join(reports_dir, 'avg_perception_municipality.png')
    plt.savefig(avg_perception_plot_path)
    plt.close(fig)

    # 2. Tabla de tendencia temporal
    temporal_trend = df.groupby('PERIODO').agg(
        PCT_SEGUROS=('PCT_SEGUROS', 'mean'),
        PCT_INSEGUROS=('PCT_INSEGUROS', 'mean'),
        TOTAL_REGISTROS=('TOTAL_REGISTROS', 'sum')
    ).reset_index()
    
    temporal_trend['PCT_SEGUROS'] = temporal_trend['PCT_SEGUROS'] * 100
    temporal_trend['PCT_INSEGUROS'] = temporal_trend['PCT_INSEGUROS'] * 100

    # Sort by year and quarter for correct temporal order
    temporal_trend['AÑO'] = temporal_trend['PERIODO'].apply(lambda x: int(x.split('-')[0]))
    temporal_trend['TRIMESTRE'] = temporal_trend['PERIODO'].apply(lambda x: int(x.split('Q')[1]))
    temporal_trend = temporal_trend.sort_values(by=['AÑO', 'TRIMESTRE']).drop(columns=['AÑO', 'TRIMESTRE'])

    temporal_trend_html = temporal_trend.to_html(index=False, classes='temporal-table', border=0, float_format='%.2f')
    temporal_trend_html_path = os.path.join(reports_dir, 'temporal_trend_table.html')
    with open(temporal_trend_html_path, 'w') as f:
        f.write(temporal_trend_html)

if __name__ == '__main__':
    generate_additional_report_assets()
