import pandas as pd
import glob
import os
import re
import unicodedata

def normalize_text(text):
    """Converts text to lowercase and removes accents."""
    if not isinstance(text, str):
        return text
    # NFD decomposes characters into base characters and diacritics
    # (e.g., 'é' -> 'e' + '´')
    # Then we filter out the diacritics (Mn category)
    nfkd_form = unicodedata.normalize('NFD', text)
    only_ascii = ''.join([c for c in nfkd_form if not unicodedata.combining(c)])
    return only_ascii.lower()

def get_quarter_from_month(month):
    """Calculates the quarter (1-4) from a given month (1-12)."""
    return (month - 1) // 3 + 1

def extract_date_info(path):
    """
    Extracts year and quarter from a file path using multiple regex patterns.
    """
    path_lower = path.lower()
    
    # Pattern 1: ...ensu_2018_1t...
    match = re.search(r"ensu_(\d{4})_(\d)t", path_lower)
    if match:
        return match.group(1), match.group(2)

    # Pattern 2: ...ensu_cb_0318... (Mar 2018)
    match = re.search(r"ensu_cb_(\d{2})(\d{2})", path_lower)
    if match:
        month, year_short = int(match.group(1)), match.group(2)
        year = f"20{year_short}"
        quarter = get_quarter_from_month(month)
        return str(year), str(quarter)

    # Pattern 3: ...ensu_01_2015...
    match = re.search(r"ensu_(\d{2})_(\d{4})", path_lower)
    if match:
        month, year = int(match.group(1)), match.group(2)
        quarter = get_quarter_from_month(month)
        return str(year), str(quarter)
        
    return None, None

def process_data():
    """
    Finds, validates, filters, and processes datasets to generate aggregated results
    for specific municipalities in Yucatán.
    """
    base_path = "data"
    output_dir = "processed_data"
    target_ent = "yucatan"
    target_municipalities = ["merida", "uman", "kanasin", "hunucma"]
    
    search_pattern = os.path.join(base_path, "**", "*cb*.csv")
    dataset_paths = glob.glob(search_pattern, recursive=True)
    
    print(f"Found {len(dataset_paths)} potential datasets. Starting robust processing...")
    
    for path in dataset_paths:
        print(f"\nProcessing file: {path}")
        
        try:
            # --- 1. Extract Year and Quarter (Robust) ---
            year, quarter = extract_date_info(path)
            if not year or not quarter:
                print(f"  - WARNING: Could not extract Year/Quarter from path. Skipping.")
                continue
            
            # --- 2. Read and Validate Columns ---
            try:
                df = pd.read_csv(path, encoding='latin1', low_memory=False)
            except Exception as e:
                print(f"  - ERROR: Could not read file {path}. Error: {e}. Skipping.")
                continue

            required_cols = ["NOM_ENT", "NOM_MUN", "BP1_1"]
            if not all(col in df.columns for col in required_cols):
                print(f"  - WARNING: File is missing one or more required columns {required_cols}. Skipping.")
                continue

            # --- 3. Filter by Entity (Yucatán) and Municipality (Robust) ---
            df['NOM_ENT_norm'] = df['NOM_ENT'].apply(normalize_text)
            df['NOM_MUN_norm'] = df['NOM_MUN'].apply(normalize_text)
            
            df_filtered = df[
                (df['NOM_ENT_norm'] == target_ent) &
                (df['NOM_MUN_norm'].isin(target_municipalities))
            ].copy()
            
            if df_filtered.empty:
                print(f"  - INFO: File contains no data for target entity/municipalities. Skipping.")
                continue

            # --- 4. Validate BP1_1 Data Range ---
            valid_bp1_1 = df_filtered["BP1_1"].isin([1, 2, 9])
            if not valid_bp1_1.any():
                print(f"  - INFO: No valid BP1_1 data (1, 2, 9) found for target municipalities. Skipping.")
                continue
            
            df_processed = df_filtered[valid_bp1_1]

            # --- 5. Group and Aggregate ---
            agg_df = df_processed.groupby(["NOM_ENT", "NOM_MUN"]).apply(lambda x: pd.Series({
                "TOTAL_REGISTROS": len(x),
                "TOTAL_SEGUROS": (x["BP1_1"] == 1).sum(),
                "TOTAL_INSEGUROS": (x["BP1_1"] == 2).sum(),
                "TOTAL_NO_RESPONDE": (x["BP1_1"] == 9).sum()
            })).reset_index()

            # --- 6. Calculate Percentages ---
            agg_df["PCT_SEGUROS"] = agg_df["TOTAL_SEGUROS"] / agg_df["TOTAL_REGISTROS"]
            agg_df["PCT_INSEGUROS"] = agg_df["TOTAL_INSEGUROS"] / agg_df["TOTAL_REGISTROS"]
            agg_df["PCT_NO_RESPONDE"] = agg_df["TOTAL_NO_RESPONDE"] / agg_df["TOTAL_REGISTROS"]
            
            # --- 7. Add Year and Quarter ---
            agg_df["AÑO"] = year
            agg_df["TRIMESTRE"] = quarter

            # --- 8. Save Output ---
            output_filename = f"resultado_{year}_{quarter}.csv"
            output_path = os.path.join(output_dir, output_filename)
            
            # Check if file exists to append or write new
            if os.path.exists(output_path):
                # Append without header
                agg_df.to_csv(output_path, mode='a', header=False, index=False)
                print(f"  - SUCCESS: Appended processed data to {output_path}")
            else:
                # Write new file with header
                agg_df.to_csv(output_path, index=False)
                print(f"  - SUCCESS: Processed data saved to {output_path}")

        except Exception as e:
            print(f"  - FATAL ERROR: An unexpected error occurred while processing {path}. Error: {e}. Skipping.")

def clean_yucatan_data():
    """
    Cleans the CSV files in data/yucatan-inseguridad, keeping only data for specific municipalities,
    and saves the cleaned files to the processed_data directory.
    """
    input_dir = "data/yucatan-inseguridad"
    output_dir = "processed_data"
    municipalities_to_keep = ["MERIDA", "UMAN", "KANASIN", "PROGRESO"]

    # Clear the output directory
    if os.path.exists(output_dir):
        for f in glob.glob(os.path.join(output_dir, '*')) :
            os.remove(f)

    # Process each file
    for filename in os.listdir(input_dir):
        if filename.endswith(".csv"):
            input_file = os.path.join(input_dir, filename)
            try:
                df = pd.read_csv(input_file)
                df.columns = [col.strip().upper() for col in df.columns]
                
                if 'NOM_MUN' in df.columns:
                    cleaned_df = df[df['NOM_MUN'].str.upper().isin(municipalities_to_keep)]
                    
                    if not cleaned_df.empty:
                        output_file = os.path.join(output_dir, filename)
                        cleaned_df.to_csv(output_file, index=False)
                        print(f"Archivo limpiado y guardado en: {output_file}")
                    else:
                        print(f"No se encontraron datos para los municipios especificados en {filename}.")
                else:
                    print(f"La columna 'NOM_MUN' no se encontró en {filename}.")

            except Exception as e:
                print(f"Error procesando el archivo {filename}: {e}")


if __name__ == "__main__":
    clean_yucatan_data()
