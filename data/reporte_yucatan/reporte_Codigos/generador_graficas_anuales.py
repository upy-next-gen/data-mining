
import pandas as pd
import matplotlib.pyplot as plt
import glob
import os
import unicodedata
import logging

def setup_logging():
    """Configura el logging para este script específico."""
    log_file = 'generacion_graficas.log'
    if logging.getLogger().hasHandlers():
        logging.getLogger().handlers.clear()

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, mode='w', encoding='utf-8'),
            logging.StreamHandler()
        ]
    )

def normalize_string(s):
    """Normaliza un string: mayúsculas, sin acentos, Ñ a N, sin espacios extra."""
    if not isinstance(s, str):
        return s
    s = s.upper().strip()
    s = s.replace('Ñ', 'N')
    s = ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn')
    return s

def main():
    """Función principal para consolidar datos, calcular promedios y generar gráficas."""
    setup_logging()
    
    input_dir = os.path.join('data', 'reporte_yucatan')
    output_dir = 'resultados_graficas'
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        logging.info(f"Directorio de salida creado en: {output_dir}")

    search_pattern = os.path.join(input_dir, 'procesado_*_yucatan.csv')
    all_files = glob.glob(search_pattern)
    
    if not all_files:
        logging.error(f"No se encontraron archivos en {input_dir}. Abortando.")
        return

    logging.info(f"Consolidando {len(all_files)} archivos de datos procesados.")
    df = pd.concat([pd.read_csv(f) for f in all_files], ignore_index=True)

    logging.info("Calculando el promedio anual ponderado de inseguridad por municipio.")
    annual_summary = df.groupby(['AÑO', 'NOM_MUN']).agg(
        TOTAL_INSEGUROS_ANUAL=('TOTAL_INSEGUROS', 'sum'),
        TOTAL_REGISTROS_ANUAL=('TOTAL_REGISTROS', 'sum')
    ).reset_index()

    annual_summary['PROMEDIO_ANUAL_INSEGURIDAD'] = (
        (annual_summary['TOTAL_INSEGUROS_ANUAL'] / annual_summary['TOTAL_REGISTROS_ANUAL']) * 100
    ).round(2)

    municipalities = annual_summary['NOM_MUN'].unique()
    logging.info(f"Generando {len(municipalities)} gráficas para los siguientes municipios: {', '.join(municipalities)}")

    for mun in municipalities:
        mun_data = annual_summary[annual_summary['NOM_MUN'] == mun].sort_values('AÑO')
        
        plt.style.use('seaborn-v0_8-whitegrid')
        fig, ax = plt.subplots(figsize=(12, 7))
        
        ax.plot(mun_data['AÑO'], mun_data['PROMEDIO_ANUAL_INSEGURIDAD'], marker='o', linestyle='-', color='b')
        
        for i, txt in enumerate(mun_data['PROMEDIO_ANUAL_INSEGURIDAD']):
            ax.annotate(f'{txt}%', (mun_data['AÑO'].iloc[i], mun_data['PROMEDIO_ANUAL_INSEGURIDAD'].iloc[i]), 
                        textcoords="offset points", xytext=(0,10), ha='center')

        ax.set_title(f'Percepción Anual de Inseguridad en {mun}', fontsize=16, weight='bold')
        ax.set_xlabel('Año', fontsize=12)
        ax.set_ylabel('Promedio de Percepción de Inseguridad (%)', fontsize=12)
        ax.set_ylim(0, 100)
        ax.set_xticks(mun_data['AÑO'].unique())
        ax.tick_params(axis='x', rotation=45)
        ax.grid(True, which='both', linestyle='--', linewidth=0.5)
        
        plt.tight_layout()

        safe_mun_name = mun.replace(' ', '_').lower()
        output_path = os.path.join(output_dir, f'{safe_mun_name}_inseguridad_anual.png')
        
        try:
            plt.savefig(output_path)
            logging.info(f"Gráfica guardada exitosamente en: {output_path}")
        except Exception as e:
            logging.error(f"No se pudo guardar la gráfica para {mun}. Error: {e}")
        
        plt.close(fig)

    logging.info("--- Proceso de generación de gráficas finalizado. ---")

if __name__ == '__main__':
    main()
