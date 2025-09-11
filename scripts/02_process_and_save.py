import pandas as pd
import re
import os
import json

def process_and_save():
    # Read the list of valid files
    try:
        with open('temp/valid_files.json', 'r') as f:
            valid_files = json.load(f)
    except FileNotFoundError:
        print("Error: temp/valid_files.json not found. Please run 01_discover_and_verify.py first.")
        return

    output_dir = 'data/yucatan_processed'
    os.makedirs(output_dir, exist_ok=True)

    def get_period_from_path(path):
        path = path.lower()
        match = re.search(r'_(\d{4})_(\d)t', path)
        if match:
            return int(match.group(1)), int(match.group(2))
        match = re.search(r'_cb_ensu_(\d{2})_(\d{4})', path)
        if match:
            month = int(match.group(1))
            year = int(match.group(2))
            quarter = (month - 1) // 3 + 1
            return year, quarter
        match = re.search(r'ensu_cb_(\d{2})(\d{2})', path)
        if match:
            month = int(match.group(1))
            year = int("20" + match.group(2))
            quarter = (month - 1) // 3 + 1
            return year, quarter
        year_match = re.search(r'(\d{4})', path)
        trim_match = re.search(r'(\d)t', path)
        year = int(year_match.group(1)) if year_match else 0
        trim = int(trim_match.group(1)) if trim_match else 0
        return year, trim

    for file_path in valid_files:
        file_path = file_path.strip()
        try:
            año, trimestre = get_period_from_path(file_path)
            if año == 0:
                print(f"WARN: Could not determine year for {file_path}. Skipping.")
                continue

            try:
                df = pd.read_csv(file_path, encoding='utf-8')
            except UnicodeDecodeError:
                df = pd.read_csv(file_path, encoding='latin-1')

            agg_df = df.groupby(['NOM_ENT', 'NOM_MUN']).agg(
                TOTAL_REGISTROS=('BP1_1', 'size'),
                TOTAL_SEGUROS=('BP1_1', lambda x: (x == 1).sum()),
                TOTAL_INSEGUROS=('BP1_1', lambda x: (x == 2).sum()),
                TOTAL_NO_RESPONDE=('BP1_1', lambda x: (x == 9).sum())
            ).reset_index()

            agg_df['PCT_SEGUROS'] = agg_df['TOTAL_SEGUROS'] / agg_df['TOTAL_REGISTROS']
            agg_df['PCT_INSEGUROS'] = agg_df['TOTAL_INSEGUROS'] / agg_df['TOTAL_REGISTROS']
            agg_df['PCT_NO_RESPONDE'] = agg_df['TOTAL_NO_RESPONDE'] / agg_df['TOTAL_REGISTROS']
            agg_df['AÑO'] = año
            agg_df['TRIMESTRE'] = trimestre

            final_df = agg_df[[
                'NOM_ENT', 'NOM_MUN', 'TOTAL_REGISTROS', 'TOTAL_SEGUROS',
                'TOTAL_INSEGUROS', 'TOTAL_NO_RESPONDE', 'PCT_SEGUROS',
                'PCT_INSEGUROS', 'PCT_NO_RESPONDE', 'AÑO', 'TRIMESTRE'
            ]]
            
            parent_dir_name = os.path.basename(os.path.dirname(os.path.dirname(file_path)))
            sanitized_name = re.sub(r'[^a-zA-Z0-9_-]', '', parent_dir_name)
            output_filename = f'procesado_{sanitized_name}.csv'

            output_path = os.path.join(output_dir, output_filename)
            final_df.to_csv(output_path, index=False, encoding='utf-8')
            print(f"Successfully processed and saved: {output_path}")

        except Exception as e:
            print(f"Could not process file {file_path}. Error: {e}")

if __name__ == '__main__':
    process_and_save()
