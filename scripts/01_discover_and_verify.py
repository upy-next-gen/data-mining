import os
import re
import json
import glob

def discover_and_verify():
    # Use glob to find all candidate files, ignoring gitignore rules
    all_cb_files = glob.glob('data/**/*.csv', recursive=True)
    
    # Filter out noise
    filtered_files = []
    for file_path in all_cb_files:
        if '__MACOSX' not in file_path and \
           'diccionario_de_datos' not in file_path and \
           not os.path.basename(file_path).startswith('._') and \
           'conjunto_de_datos' in file_path:
            filtered_files.append(os.path.abspath(file_path))

    # Verify headers
    required_columns = ['NOM_ENT', 'NOM_MUN', 'BP1_1']
    valid_files = []
    invalid_files = []
    for file_path in filtered_files:
        file_path = file_path.strip()
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                header = f.readline()
                if all(col in header for col in required_columns):
                    valid_files.append(file_path)
                else:
                    invalid_files.append(file_path)
        except Exception as e:
            invalid_files.append(f"{file_path} (Error: {e})")

    # Save valid files to a temporary file
    os.makedirs('temp', exist_ok=True)
    with open('temp/valid_files.json', 'w') as f:
        json.dump(valid_files, f, indent=4)

    print(f"Phase 1 & 2 Complete.")
    print(f"Found {len(valid_files)} valid files.")
    print(f"List of valid files saved to temp/valid_files.json")

if __name__ == '__main__':
    discover_and_verify()
