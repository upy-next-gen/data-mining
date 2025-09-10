import pandas as pd

def analyze_data():
    """
    Analyzes the processed data to check the values in the BP1_1 column.
    """
    input_file = "processed_data.csv"
    
    try:
        df = pd.read_csv(input_file)
        
        # Get unique values from the BP1_1 column
        unique_values = df['BP1_1'].unique()
        
        print(f"Unique values in BP1_1 column: {unique_values}")
        
        # Check if all unique values are within the expected set
        expected_values = {1, 2, 9}
        is_valid = all(value in expected_values for value in unique_values)
        
        if is_valid:
            print("All values in BP1_1 are within the expected range of {1, 2, 9}.")
        else:
            print("Warning: Some values in BP1_1 are outside the expected range of {1, 2, 9}.")
            
    except FileNotFoundError:
        print(f"Error: The file {input_file} was not found.")
    except KeyError:
        print("Error: The 'BP1_1' column was not found in the input file.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    analyze_data()
