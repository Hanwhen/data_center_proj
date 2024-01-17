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
array_truth = [[0 for _ in range(len(list_csv_file))] for _ in range(len(set_col))]



# Output the truth table
for row, curr_header in enumerate(set_col):
    set_col_response = set()
    for col, ddf in enumerate(ddf_list):
        # Check if the lowercase header is in the current CSV file
        # ddf.columns.map(str.casefold) projects str.casefold (ignore case) to each column name
        # Need to convert the result to a list since ddf.columns.map returns a object
        if curr_header in ddf.columns.map(str.casefold).tolist(): 
            # set_col_response.add(curr_header)
            array_truth[row][col] = 1
    # print(set_col_response)
# Create a DataFrame from the array_truth with list_csv as columns and set_col as index
df_truth = pd.DataFrame(array_truth, columns=[file_name.replace('data/PAYS_', '') for file_name in list_csv_file], index=set_col)

# Save the DataFrame to a CSV file
df_truth.to_csv('truth_table_ignore_case.csv', index_label='')
