import os
import pandas as pd
import dask.dataframe as dd

############################################################################
# NOTE: Do not put any folder under "data" folder in the root directory
#       All CSV files must be directly under the "data" folder
#       Do not open any CSV file while running this script
############################################################################

# Store all CSV files under the "data" folder in the root directory into csv_list
root_dir = "data/"
list_csv_file = [os.path.join(root, filename) for root, _, files in os.walk(root_dir) for filename in files if filename.endswith('.csv')]

# Set to store lowercased column names
set_col = set()

# Create a Dask DataFrame
ddf_list = [dd.read_csv(csv_file) for csv_file in list_csv_file]

# Process the Dask DataFrames and collect column names (ignoring case)
for i, ddf in enumerate(ddf_list):
    for column in ddf.columns:
        # Append lowercase headers of all CSV files to set_col
        set_col.add(column.lower())

# Sort the set of all column names
set_col = sorted(set_col)

# Create a 2D array to store the truth table
array_truth = [[None for _ in range(len(list_csv_file))] for _ in range(len(set_col))]

# Process each CSV file to populate the truth table
for col, csv_file in enumerate(list_csv_file):
    df = pd.read_csv(csv_file)
    for row, curr_header in enumerate(set_col):
        matched_columns = [col_name for col_name in df.columns if col_name.lower() == curr_header]
        if matched_columns:
            try:
                # Column exists in this CSV file
                col_data = df[matched_columns[0]].unique()
                array_truth[row][col] = ', '.join(map(str, col_data))
            except Exception as e:
                print(f"Error processing column {curr_header} in {csv_file}: {e}")

# Create a DataFrame from the array_truth with list_csv as columns and set_col as index
df_truth = pd.DataFrame(array_truth, columns=[file_name.replace('data/PAYS_', '') for file_name in list_csv_file], index=set_col)

# Save the DataFrame to a CSV file
df_truth.to_csv('truth_table_user_input.csv', index_label='')

print("Truth table has been saved to 'truth_table_user_input.csv'")
