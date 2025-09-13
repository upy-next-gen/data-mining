import os
import pandas as pd
import re
import logging
import shutil
import glob
from typing import List, Optional, Tuple, Dict

# --- Configuración de Artefactos del Proyecto ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ORIGEN_DATOS_DIR = os.path.join(BASE_DIR, 'dataru')
INTERMEDIOS_DIR = os.path.join(BASE_DIR, 'data', 'yucatan-inseguridad')
LOG_FILE = os.path.join(BASE_DIR, 'procesamiento_log.txt')
FINAL_OUTPUT_FILE = os.path.join(BASE_DIR, 'dataset_procesado_final.csv')

# --- Configuración del Logger ---
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(LOG_FILE, mode='w', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

def preparar_entorno():
    """Fase 0: Prepara el entorno, limpiando y creando directorios necesarios."""
    logging.info("--- FASE 0: Preparación del Entorno ---")
    if os.path.exists(INTERMEDIOS_DIR):
        shutil.rmtree(INTERMEDIOS_DIR)
        logging.info(f"Directorio de resultados intermedios '{INTERMEDIOS_DIR}' limpiado.")
    os.makedirs(INTERMEDIOS_DIR)
    logging.info(f"Directorio de resultados intermedios '{INTERMEDIOS_DIR}' creado.")

def descubrir_archivos() -> List[str]:
    """Fase 1.1: Descubre archivos a procesar buscando múltiples patrones de nomenclatura."""
    logging.info("--- FASE 1: Descubrimiento e Ingestión de Archivos ---")
    
    # Patrón para la nomenclatura antigua (ej: conjunto_de_datos_cb_ensu_01_2015.csv)
    patron_antiguo = '**/conjunto_de_datos_cb_*.csv'
    # Patrón para la nomenclatura nueva (ej: conjunto_de_datos_ensu_cb_0623.csv)
    patron_nuevo = '**/conjunto_de_datos_ensu_cb_*.csv'

    logging.info(f"Buscando archivos con patrones: '{patron_antiguo}' y '{patron_nuevo}'")

    archivos_antiguos = glob.glob(os.path.join(ORIGEN_DATOS_DIR, patron_antiguo), recursive=True)
    archivos_nuevos = glob.glob(os.path.join(ORIGEN_DATOS_DIR, patron_nuevo), recursive=True)
    
    archivos_candidatos = archivos_antiguos + archivos_nuevos
    
    # Usar un set para eliminar duplicados si algún archivo coincide con ambos patrones
    archivos_candidatos = sorted(list(set(archivos_candidatos)))

    # Filtrar rutas que contengan '__MACOSX'
    archivos_filtrados = [p for p in archivos_candidatos if '__MACOSX' not in p]

    logging.info(f"Se encontraron {len(archivos_filtrados)} archivos de datos únicos para procesar.")
    return archivos_filtrados

def leer_archivo(ruta_archivo: str) -> Optional[pd.DataFrame]:
    """Fase 1.2: Intenta leer un archivo CSV con codificaciones UTF-8 y Latin-1."""
    try:
        df = pd.read_csv(ruta_archivo, encoding='utf-8', low_memory=False)
        logging.info(f"Archivo '{os.path.basename(ruta_archivo)}' leído exitosamente con codificación UTF-8.")
        return df
    except UnicodeDecodeError:
        logging.warning(f"Falló la lectura de '{os.path.basename(ruta_archivo)}' con UTF-8. Reintentando con Latin-1.")
        try:
            df = pd.read_csv(ruta_archivo, encoding='latin-1', low_memory=False)
            logging.info(f"Archivo '{os.path.basename(ruta_archivo)}' leído exitosamente con codificación Latin-1.")
            return df
        except Exception as e:
            logging.error(f"No se pudo leer el archivo '{os.path.basename(ruta_archivo)}' con ninguna codificación. Error: {e}")
            return None
    except Exception as e:
        logging.error(f"Error inesperado al leer el archivo '{os.path.basename(ruta_archivo)}'. Error: {e}")
        return None

def detectar_y_transformar_esquema(df: pd.DataFrame, ruta_archivo: str) -> Optional[pd.DataFrame]:
    """
    Detecta el esquema del DataFrame (Moderno, 2016, 2015) y lo transforma al formato moderno.
    La lógica para formatos antiguos está hardcodeada según el análisis de datos.
    """
    logging.info(f"Analizando esquema para {os.path.basename(ruta_archivo)}...")
    df.columns = [str(col).upper() for col in df.columns]

    # --- Estrategia 1: Esquema Moderno (Post-2016) ---
    if 'NOM_ENT' in df.columns and 'BP1_1' in df.columns:
        logging.info("Esquema moderno detectado. No se requiere transformación.")
        return df

    # --- Estrategia 2: Esquema 2016 ---
    # Lógica: CD=52 es Mérida, Yucatán. La pregunta es BP1_1.
    if 'BP1_1' in df.columns and 'CD' in df.columns:
        # Asegurarse que la columna CD es de tipo string para la comparación
        df['CD'] = df['CD'].astype(str)
        df_merida = df[df['CD'] == '52'].copy()
        if not df_merida.empty:
            logging.info("Esquema antiguo (tipo 2016) detectado. Aplicando transformación hardcodeada.")
            df_merida['NOM_ENT'] = 'YUCATAN'
            df_merida['NOM_MUN'] = 'MERIDA'
            return df_merida

    # --- Estrategia 3: Esquema 2015 ---
    # Lógica: CD=08 es Mérida, Yucatán. La pregunta es P1.
    if 'P1' in df.columns and 'CD' in df.columns:
        # Asegurarse que la columna CD es de tipo string para la comparación
        df['CD'] = df['CD'].astype(str)
        df_merida = df[df['CD'] == '8'].copy() # El '08' puede ser leído como 8
        if not df_merida.empty:
            logging.info("Esquema antiguo (tipo 2015) detectado. Aplicando transformación hardcodeada.")
            df_merida = df_merida.rename(columns={'P1': 'BP1_1'})
            df_merida['NOM_ENT'] = 'YUCATAN'
            df_merida['NOM_MUN'] = 'MERIDA'
            return df_merida

    logging.warning(f"Esquema no reconocido o no aplicable para Yucatán en el archivo '{os.path.basename(ruta_archivo)}'. Columnas: {df.columns.tolist()}")
    return None

def validar_y_limpiar(df: pd.DataFrame, ruta_archivo: str) -> Optional[pd.DataFrame]:
    """Fase 2: Aplica validaciones de esquema, filtro geográfico y limpieza de valores."""
    columnas_requeridas = ['NOM_ENT', 'NOM_MUN', 'BP1_1']
    if not all(col in df.columns for col in columnas_requeridas):
        logging.warning(f"Archivo '{os.path.basename(ruta_archivo)}' descartado post-transformación: faltan columnas. Requeridas: {columnas_requeridas}. Encontradas: {df.columns.tolist()}")
        return None

    df['NOM_ENT'] = df['NOM_ENT'].astype(str).str.strip().str.upper().str.replace('Ñ', 'N', regex=False)
    df_yucatan = df[df['NOM_ENT'] == 'YUCATAN'].copy()
    if df_yucatan.empty:
        logging.info(f"Archivo '{os.path.basename(ruta_archivo)}' no contiene registros para Yucatán.")
        return None

    filas_antes = len(df_yucatan)
    df_yucatan['BP1_1'] = pd.to_numeric(df_yucatan['BP1_1'], errors='coerce')
    df_validado = df_yucatan[df_yucatan['BP1_1'].isin([1, 2, 9])].copy()
    
    if len(df_validado) < filas_antes:
        logging.warning(f"En '{os.path.basename(ruta_archivo)}', se excluyeron {filas_antes - len(df_validado)} filas por valores inválidos en 'BP1_1'.")

    return df_validado if not df_validado.empty else None

def procesar_y_agregar(df: pd.DataFrame) -> pd.DataFrame:
    """Fase 3: Normaliza, agrupa y calcula métricas."""
    df['NOM_MUN'] = df['NOM_MUN'].astype(str).str.strip().str.upper().str.replace('Ñ', 'N', regex=False)
    agregado = df.groupby(['NOM_ENT', 'NOM_MUN']).agg(
        TOTAL_REGISTROS=('BP1_1', 'size'),
        TOTAL_SEGUROS=('BP1_1', lambda x: (x == 1).sum()),
        TOTAL_INSEGUROS=('BP1_1', lambda x: (x == 2).sum()),
        TOTAL_NO_RESPONDE=('BP1_1', lambda x: (x == 9).sum())
    ).reset_index()
    agregado['PCT_SEGUROS'] = (agregado['TOTAL_SEGUROS'] / agregado['TOTAL_REGISTROS'] * 100).fillna(0)
    agregado['PCT_INSEGUROS'] = (agregado['TOTAL_INSEGUROS'] / agregado['TOTAL_REGISTROS'] * 100).fillna(0)
    agregado['PCT_NO_RESPONDE'] = (agregado['TOTAL_NO_RESPONDE'] / agregado['TOTAL_REGISTROS'] * 100).fillna(0)
    return agregado

def enriquecer_y_guardar(df: pd.DataFrame, ruta_archivo: str):
    """Fase 4: Extrae metadatos (año, trimestre) y guarda el resultado intermedio."""
    año, trimestre = 9999, 9
    nombre_base = os.path.basename(ruta_archivo)

    # Patrón 1: Formato YYYY_Qt (ej. ...2018_3t...)
    match1 = re.search(r'(\d{4})_(\d)t', nombre_base, re.I)
    # Patrón 2: Formato ensu_MM_YYYY (ej. ...ensu_03_2018...)
    match2 = re.search(r'ensu_(\d{2})_(\d{4})', nombre_base, re.I)
    # Patrón 3: Formato ensu_cb_MMYY (ej. ...ensu_cb_0623.csv)
    match3 = re.search(r'ensu_cb_(\d{2})(\d{2})', nombre_base, re.I)

    if match1:
        g = match1.groups()
        año, trimestre = int(g[0]), int(g[1])
    elif match2:
        g = match2.groups()
        año, trimestre = int(g[1]), (int(g[0]) - 1) // 3 + 1
    elif match3:
        g = match3.groups()
        mes, año_corto = int(g[0]), int(g[1])
        año = 2000 + año_corto
        trimestre = (mes - 1) // 3 + 1

    if año == 9999:
        logging.error(f"No se pudo extraer AÑO y TRIMESTRE de '{nombre_base}'. Marcado con valores por defecto.")
    
    # Añadir las columnas al DataFrame ANTES de guardar
    df['AÑO'] = año
    df['TRIMESTRE'] = trimestre

    nombre_salida = f"procesado_{año}_Q{trimestre}_{nombre_base}"
    df.to_csv(os.path.join(INTERMEDIOS_DIR, nombre_salida), index=False, encoding='utf-8')
    logging.info(f"Archivo intermedio guardado: {nombre_salida}")

def consolidar_resultados():
    """Fase 5: Consolida todos los archivos intermedios."""
    logging.info("--- FASE 5: Consolidación Final ---")
    archivos_intermedios = glob.glob(os.path.join(INTERMEDIOS_DIR, "procesado_*.csv"))
    if not archivos_intermedios:
        logging.warning("No se generaron archivos intermedios. El dataset final estará vacío.")
        pd.DataFrame(columns=['NOM_ENT', 'NOM_MUN', 'TOTAL_REGISTROS', 'TOTAL_SEGUROS', 'TOTAL_INSEGUROS', 'TOTAL_NO_RESPONDE', 'PCT_SEGUROS', 'PCT_INSEGUROS', 'PCT_NO_RESPONDE', 'AÑO', 'TRIMESTRE']).to_csv(FINAL_OUTPUT_FILE, index=False, encoding='utf-8')
        return

    df_consolidado = pd.concat([pd.read_csv(f) for f in archivos_intermedios], ignore_index=True)
    df_consolidado.to_csv(FINAL_OUTPUT_FILE, index=False, encoding='utf-8')
    logging.info(f"Dataset maestro final guardado en '{FINAL_OUTPUT_FILE}'.")

def main():
    """Orquesta todo el proceso de ETL."""
    preparar_entorno()
    archivos = descubrir_archivos()
    for archivo in archivos:
        df_raw = leer_archivo(archivo)
        if df_raw is None or df_raw.empty:
            continue
        
        df_transformado = detectar_y_transformar_esquema(df_raw, archivo)
        if df_transformado is None:
            continue

        df_validado = validar_y_limpiar(df_transformado, archivo)
        if df_validado is None:
            continue
            
        df_agregado = procesar_y_agregar(df_validado)
        enriquecer_y_guardar(df_agregado, archivo)

    consolidar_resultados()
    logging.info("--- Proceso de ETL completado exitosamente. ---")

if __name__ == "__main__":
    main()
