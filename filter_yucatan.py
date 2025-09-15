
import pandas as pd
import os

def main():
    """
    Reads the processed data, filters it for entries from Yucatan,
    and saves the result to a new CSV file.
    """
    # Define file paths
    input_path = os.path.join("processed data", "processed_data.csv")
    output_path = os.path.join("processed data", "yucatan_data.csv")

    print(f"Reading data from {input_path}...")

    # Check if the input file exists
    if not os.path.exists(input_path):
        print(f"Error: Input file not found at {input_path}")
        return

    # Read the processed CSV file
    df = pd.read_csv(input_path)

    print("Filtering data for NOM_ENT == 'YUCATAN'...")
    
    # Filter the DataFrame
    yucatan_df = df[df['NOM_ENT'] == 'YUCATAN'].copy()

    if yucatan_df.empty:
        print("Warning: No entries for 'YUCATAN' found in the data.")
    else:
        print(f"Found {len(yucatan_df)} entries for Yucatan.")

    print(f"Saving filtered data to {output_path}...")
    
    # Save the filtered dataframe to a new CSV file
    yucatan_df.to_csv(output_path, index=False)

    print("Filtering complete. File saved successfully.")

if __name__ == "__main__":
    main()
