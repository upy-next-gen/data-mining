
import pandas as pd
import os
import glob
import unicodedata

def normalize_text(text):
    if isinstance(text, str):
        text = text.upper()
        text = ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')
        text = text.replace('Ñ', 'N')
    return text

def process_csv(file_path, log_file):
    try:
        df = pd.read_csv(file_path, encoding='latin1', low_memory=False)
        
        required_cols = ['NOM_ENT', 'NOM_MUN', 'BP1_1']
        if not all(col in df.columns for col in required_cols):
            log_file.write(f"Skipping {file_path}: Missing one or more required columns.\n")
            return None
            
        df['NOM_ENT'] = df['NOM_ENT'].apply(normalize_text)
        df = df[df['NOM_ENT'] == 'YUCATAN']
        
        if df.empty:
            log_file.write(f"Skipping {file_path}: No data for YUCATAN.\n")
            return None
            
        df['NOM_MUN'] = df['NOM_MUN'].apply(normalize_text)
        
        year, quarter = extract_year_quarter(file_path)
        
        df['AÑO'] = year
        df['TRIMESTRE'] = quarter
        
        return df[['AÑO', 'TRIMESTRE', 'NOM_MUN', 'BP1_1']]
        
    except Exception as e:
        log_file.write(f"Error processing {file_path}: {e}\n")
        return None

def extract_year_quarter(file_path):
    parts = file_path.split('_')
    year = None
    quarter = None
    
    for i, part in enumerate(parts):
        if part.isdigit() and len(part) == 4:
            year = part
            if i > 0 and parts[i-1].isdigit():
                quarter = parts[i-1]
            break
        elif 'ensu' in part and len(part) > 4 and part[4:].isdigit():
             year = part[4:]
             if len(year) == 4:
                quarter = part[2:4]
        elif part.isdigit() and len(part) == 2:
            if int(part) > 10: #likely a year
                year = "20" + part
            else: #likely a quarter
                quarter = part

    if year and not quarter:
        for part in parts:
            if 't' in part and len(part) == 2 and part[0].isdigit():
                quarter = part[0]
                break
    
    if not year or not quarter:
        # Fallback for names like "ensu_cb_0118"
        for part in parts:
            if len(part) == 4 and part.isdigit():
                month = int(part[:2])
                year = "20" + part[2:]
                quarter = (month - 1) // 3 + 1
                break

    return year, quarter


def main():
    data_path = 'C:\\Users\\Daniel Gomez\\Documents\\chris-mining\\data-mining-main\\archives'
    output_dir = 'C:\\Users\\Daniel Gomez\\Documents\\chris-mining\\data\\yucatan-inseguridad'
    log_dir = 'C:\\Users\\Daniel Gomez\\Documents\\chris-mining\\logs\\phase2'
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
        
    log_file_path = os.path.join(log_dir, 'processing.log')
    
    with open(log_file_path, 'w') as log_file:
        all_files = glob.glob(os.path.join(data_path, '**', '*cb*.csv'), recursive=True)
        
        processed_dfs = []
        
        for file_path in all_files:
            if '__MACOSX' not in file_path and '._' not in os.path.basename(file_path) and 'diccionario_de_datos' not in file_path:
                log_file.write(f"Processing {file_path}...")
                processed_df = process_csv(file_path, log_file)
                if processed_df is not None:
                    processed_dfs.append(processed_df)
                    
        if processed_dfs:
            consolidated_df = pd.concat(processed_dfs, ignore_index=True)
            
            # Further processing
            consolidated_df.rename(columns={'BP1_1': 'PERCEPCION'}, inplace=True)
            
            perception_map = {1: 'SEGURO', 2: 'INSEGURO'}
            consolidated_df['PERCEPCION'] = consolidated_df['PERCEPCION'].map(perception_map).fillna('NO_RESPONDE')
            
            summary = consolidated_df.groupby(['AÑO', 'TRIMESTRE', 'NOM_MUN', 'PERCEPCION']).size().unstack(fill_value=0).reset_index()
            
            summary['TOTAL_REGISTROS'] = summary['SEGURO'] + summary['INSEGURO'] + summary.get('NO_RESPONDE', 0)
            
            summary['PCT_SEGUROS'] = (summary['SEGURO'] / summary['TOTAL_REGISTROS']) * 100
            summary['PCT_INSEGUROS'] = (summary['INSEGURO'] / summary['TOTAL_REGISTROS']) * 100
            summary['PCT_NO_RESPONDE'] = (summary.get('NO_RESPONDE', 0) / summary['TOTAL_REGISTROS']) * 100
            
            output_file = os.path.join(output_dir, 'yucatan_inseguridad_consolidado.csv')
            summary.to_csv(output_file, index=False)
            
            log_file.write(f"Consolidated file saved to {output_file}")
        else:
            log_file.write("No data processed.")

if __name__ == '__main__':
    main()
