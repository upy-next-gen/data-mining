
import pandas as pd
import matplotlib.pyplot as plt
import glob
import os
import logging

def setup_logging():
    """Configura el logging."""
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                        handlers=[logging.FileHandler('grafica_promedio.log', mode='w'), logging.StreamHandler()])

def main():
    """Genera la gráfica del promedio estatal."""
    setup_logging()
    
    input_dir = os.path.join('data', 'reporte_yucatan')
    output_dir = 'resultados_graficas'
    
    search_pattern = os.path.join(input_dir, 'procesado_*_yucatan.csv')
    all_files = glob.glob(search_pattern)
    
    if not all_files:
        logging.error(f"No se encontraron archivos en {input_dir}.")
        return

    df = pd.concat([pd.read_csv(f) for f in all_files], ignore_index=True)

    summary_data = []
    for period, group in df.groupby(['AÑO', 'TRIMESTRE']):
        total_inseguros = group['TOTAL_INSEGUROS'].sum()
        total_registros = group['TOTAL_REGISTROS'].sum()
        promedio = round((total_inseguros / total_registros) * 100, 2) if total_registros > 0 else 0
        summary_data.append({'Período': f"{period[0]}-Q{period[1]}", 'Promedio_Inseguridad': promedio})
    
    summary_df = pd.DataFrame(summary_data).sort_values('Período')

    plt.style.use('seaborn-v0_8-whitegrid')
    fig, ax = plt.subplots(figsize=(15, 8))
    
    ax.plot(summary_df['Período'], summary_df['Promedio_Inseguridad'], marker='o', linestyle='-', color='r')
    
    for i, txt in enumerate(summary_df['Promedio_Inseguridad']):
        ax.annotate(f'{txt}%', (summary_df['Período'].iloc[i], summary_df['Promedio_Inseguridad'].iloc[i]),
                    textcoords="offset points", xytext=(0,10), ha='center')

    ax.set_title('Promedio Ponderado de Percepción de Inseguridad en Yucatán por Período', fontsize=16, weight='bold')
    ax.set_xlabel('Período (Año-Trimestre)', fontsize=12)
    ax.set_ylabel('Promedio de Inseguridad (%)', fontsize=12)
    ax.set_ylim(0, 100)
    ax.tick_params(axis='x', rotation=45)
    ax.grid(True, which='both', linestyle='--', linewidth=0.5)
    
    plt.tight_layout()
    
    output_path = os.path.join(output_dir, 'promedio_inseguridad_yucatan_por_periodo.png')
    plt.savefig(output_path)
    logging.info(f"Gráfica de promedio estatal guardada en: {output_path}")
    plt.close(fig)

if __name__ == '__main__':
    main()
