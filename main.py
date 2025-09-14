import os
import pandas as pd
import logging

# Configure basic logging for main.py
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def process_data(input_path, output_path):
    """
    Reads a CSV file, selects specific columns, and saves it to a new CSV file.
    """
    try:
        # Read the CSV file
        df = pd.read_csv(input_path, encoding='latin1')

        # Select the desired columns
        columns_to_keep = ['NOM_ENT', 'NOM_MUN', 'NOM_CD', 'BP1_1']
        df_processed = df[columns_to_keep]


        # Save the new dataset
        df_processed.to_csv(output_path, index=False)
        logging.info(f"Processed data saved to {output_path}")

    except FileNotFoundError:
        logging.error(f"Error: The file was not found at {input_path}")
    except KeyError as e:
        logging.error(f"Error: One or more columns were not found in the CSV file: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    # Define file paths
    # Use relative path for input data
    input_csv_path = os.path.join("data", "Archive", "conjunto_de_datos_ensu_2025_2t_csv", "conjunto_de_datos_ensu_cb_0625", "conjunto_de_datos", "conjunto_de_datos_ensu_cb_0625.csv")
    
    # Ensure output directory exists
    output_dir = os.path.join("data", "processed")
    os.makedirs(output_dir, exist_ok=True)
    output_csv_path = os.path.join(output_dir, "processed_data.csv")
    
    # Process the data
    process_data(input_csv_path, output_csv_path)

    