import pandas as pd
import os

def main():
    """
    Reads a specific CSV file, selects a subset of columns,
    and saves the result to a new CSV file in a different directory.
    """
    # Define the input and output paths
    input_path = os.path.join("raw data", "conjunto_de_datos_ensu_2025_2t_csv", "conjunto_de_datos_ensu_cb_0625", "conjunto_de_datos", "conjunto_de_datos_ensu_cb_0625.csv")
    output_dir = "processed data"
    output_path = os.path.join(output_dir, "processed_data.csv")

    # Ensure the output directory exists
    os.makedirs(output_dir, exist_ok=True)

    print(f"Reading data from {input_path}...")
    
    # Check if the input file exists
    if not os.path.exists(input_path):
        print(f"Error: Input file not found at {input_path}")
        return

    # Read the CSV file
    df = pd.read_csv(input_path, encoding='latin1') # Using latin1 encoding as it is common for Spanish datasets

    print("Selecting required columns: NOM_ENT, NOM_MUN, NOM_CD, BP1_1...")
    
    # Define the columns to keep
    columns_to_keep = ["NOM_ENT", "NOM_MUN", "NOM_CD", "BP1_1"]
    
    # Check if all required columns are in the dataframe
    missing_cols = [col for col in columns_to_keep if col not in df.columns]
    if missing_cols:
        print(f"Error: The following required columns are missing from the input file: {missing_cols}")
        return
        
    new_df = df[columns_to_keep]

    print(f"Saving processed data to {output_path}...")
    
    # Save the new dataframe to a CSV file
    new_df.to_csv(output_path, index=False)

    print("Processing complete. File saved successfully.")

if __name__ == "__main__":
    main()