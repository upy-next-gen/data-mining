import pandas as pd
import os

# Define the input and output file paths
input_file = os.path.join('data', 'conjunto_de_datos_ensu_2025_2t_csv', 'conjunto_de_datos_ensu_cb_0625', 'conjunto_de_datos', 'conjunto_de_datos_ensu_cb_0625.csv')
output_file = os.path.join('data', 'processed_data.csv')

# Define the columns to select
columns_to_select = ['NOM_ENT', 'NOM_MUN', 'NOM_CD', 'BP1_1']

try:
    # Read the CSV file
    print(f"Reading data from {input_file}...")
    df = pd.read_csv(input_file)
    print("Data read successfully.")

    # Select the desired columns
    print(f"Selecting columns: {', '.join(columns_to_select)}...")
    new_df = df[columns_to_select]
    print("Columns selected successfully.")

    # Save the new DataFrame to a new CSV file
    print(f"Saving processed data to {output_file}...")
    new_df.to_csv(output_file, index=False)
    print("Processed data saved successfully.")

except FileNotFoundError:
    print(f"Error: The file was not found at {input_file}")
except KeyError as e:
    print(f"Error: One or more columns were not found in the CSV file: {e}")
except Exception as e:
    print(f"An unexpected error occurred: {e}")