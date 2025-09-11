import pandas as pd

def check_column_values(file_path, column_name, allowed_values):
    """
    Checks if the unique values in a column of a CSV file are within a specified set of allowed values.

    Args:
        file_path (str): The path to the CSV file.
        column_name (str): The name of the column to check.
        allowed_values (set): A set of allowed values.
    """
    try:
        df = pd.read_csv(file_path)
        unique_values = set(df[column_name].unique())

        if unique_values.issubset(allowed_values):
            print(f"The values in the column '{column_name}' are within the allowed range {allowed_values}.")
        else:
            print(f"The values in the column '{column_name}' are NOT within the allowed range {allowed_values}.")
            print(f"Unique values found: {unique_values}")

    except FileNotFoundError:
        print(f"Error: The file {file_path} was not found.")
    except KeyError:
        print(f"Error: The column '{column_name}' was not found in the CSV file.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    input_file = "processed_dataset.csv"
    column_to_check = "BP1_1"
    allowed_values = {1, 2, 9}

    check_column_values(input_file, column_to_check, allowed_values)
