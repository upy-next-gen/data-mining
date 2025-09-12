import pandas as pd

# Define the path to the input CSV file
input_file = r"data/conjunto_de_datos_ensu_2025_2t_csv/conjunto_de_datos_ensu_cb_0625/conjunto_de_datos/conjunto_de_datos_ensu_cb_0625.csv"

# Define the columns to keep
columns_to_keep = ["NOM_ENT", "NOM_MUN", "NOM_CD", "BP1_1"]

# Read the CSV file into a pandas DataFrame
# Using encoding='latin1' as it is common for files from Latin American government agencies.
try:
    df = pd.read_csv(input_file, usecols=columns_to_keep, encoding='latin1')
except Exception as e:
    print(f"Error reading the CSV file: {e}")
    exit()

# Define the path for the output CSV file
output_file = r"processed_data.csv"

# Save the new DataFrame to a new CSV file
df.to_csv(output_file, index=False)

print(f"Successfully created {output_file} with the selected columns.")
