import pandas as pd
import os

def process_data():
    """
    Processes the ENSU dataset to extract specific columns.
    """
    try:
        # Define the path to the input file
        input_path = os.path.join('data', 'archive', 'conjunto_de_datos_ensu_2025_2t_csv', 'conjunto_de_datos_ensu_cb_0625', 'conjunto_de_datos', 'conjunto_de_datos_ensu_cb_0625.csv')

        # Check if the file exists
        if not os.path.exists(input_path):
            print(f"Error: The file was not found at {input_path}")
            return

        # Read the CSV file
        df = pd.read_csv(input_path, encoding='latin1') # Using latin1 encoding as it's common in Spanish datasets

        # Define the columns to keep
        columns_to_keep = ['NOM_ENT', 'NOM_MUN', 'NOM_CD', 'BP1_1']

        # Check if all required columns are in the dataframe
        missing_cols = [col for col in columns_to_keep if col not in df.columns]
        if missing_cols:
            print(f"Error: The following required columns are missing from the file: {', '.join(missing_cols)}")
            return

        # Create the new dataframe
        new_df = df[columns_to_keep]

        # Define the path for the output file
        output_path = 'datos_procesados.csv'

        # Save the new dataframe to a CSV file
        new_df.to_csv(output_path, index=False)

        print(f"Data processed successfully. The new dataset is saved as '{output_path}'")

    except FileNotFoundError:
        print(f"Error: The file was not found. Please check the path.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    process_data()
