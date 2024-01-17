import os
import dask.dataframe as dd
import pandas as pd

############################################################################
# NOTE: Do not put any folder under "data" folder in the root directory
#       All CSV files must be directly under the "data" folder
#       Do not open any CSV file while running this script
############################################################################

# Store all CSV files under the "data" folder in the root directory into csv_list
root_dir = "data/"
list_csv_file = [os.path.join(root, filename) for root, _, files in os.walk(root_dir) for filename in files if filename.endswith('.csv')]

# Set to store column names
set_col = set()

# List of sets to store unique headers for each .csv file
list_col_set = [set() for _ in range(len(list_csv_file))]

# Read all CSV files using Dask
ddf_list = [dd.read_csv(csv_file) for csv_file in list_csv_file]

# Process the Dask DataFrames and collect column names
for i, ddf in enumerate(ddf_list):
    for column in ddf.columns:
        # Append headers of all CSV files to set_col
        set_col.add(column)
        # Append headers of CSV file to the corresponding set
        list_col_set[i].add(column)

# Sort the list of sets
for i in range(len(list_col_set)):
    list_col_set[i] = sorted(list_col_set[i])

# Sort the set of all column names
set_col = sorted(set_col)

# Create a 2D array to store the truth table
array_truth = [[0 for _ in range(len(list_csv_file))] for _ in range(len(set_col))]

iterator_header = iter(set_col)
iterator_col_set = iter(list_col_set)


# Output the truth table
for row in range(len(set_col)):
    next_header = next(iterator_header)
    iterator_col_set = iter(list_col_set)  # Reset the iterator
    for col in range(len(list_csv_file)):
        curr_col_set = next(iterator_col_set)
        if any(col_name == next_header for col_name in curr_col_set):
            array_truth[row][col] = 1

# Create a DataFrame from the array_truth with list_csv as columns and set_col as index
df_truth = pd.DataFrame(array_truth, columns=[file_name.replace('data/PAYS_', '') for file_name in list_csv_file], index=set_col)

# Save the DataFrame to a CSV file
df_truth.to_csv('truth_table.csv', index_label='')
