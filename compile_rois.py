import csv
import statistics as stats
import pandas as pd
import ntpath
import os
import glob
import contextlib as cl
import re
from collections import defaultdict
# Assign unique slice names for each slice, e.g., BILLY_S1, BILLY_S2, etc.
# Note: Not all samples have the same number of slices
# Ensure all slices are included, and replace any missing values with NA

# Process data in batches, e.g., DALE2 batch, FRANK batch, etc.
# Read and process files in groups of 3

# Extract the same information as before, but repeat the process for each file type
# Extract the 'Area' column for RoiNo 0 and RoiNo 1
# Calculate the mean area for RoiNo 0 and RoiNo 1

# Save the calculated values to a new Excel file
# For each ROI, retrieve the corresponding row value
# Example: The first RoiNo 0 corresponds to row number 2. Use this row to extract the 'Area' value

# Note: Two ROIs exist for each image
# Luminal areas will always have an odd-numbered index
# Using a bitwise AND operator is faster than modulo (%) for checking odd/even indices

# Output format: filename, mean luminal area, mean arterial area, mean vessel area

DIR_DATA_PATH = os.path.join("c:\\","Users\danie\Desktop\\atherosclerosis_alkystis_mri_data_analysis-main\data")
COLUMN_NAMES = [
    "SLICE_NR", "T1BB_OUTER", "T1BB_INNER", "T1BB_PLAQUE", "T1BB_CE_OUTER", "T1BB_CE_INNER", "T1BB_CE_PLAQUE",
    "IR_CE_OUTER", "IR_CE_INNER", "IR_CE_PLAQUE", "OUTCOME"
]
DIR_OUT = os.path.join("c:\\","Users\danie\Desktop\\atherosclerosis_alkystis_mri_data_analysis-main", "cleaned_data")  # Replace with the path to your output folder
COLUMN_TARGET = 'NumOfPoints'  # Replace with the target column name

def delete_columns_after(input_file, output_file, target_column):

    with open(input_file, mode='r', newline='') as infile, open(output_file, mode='w', newline='') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        
        header = next(reader)
        
        try:
            target_index = header.index(target_column)
        except ValueError:
            print(f"Column '{target_column}' not found in {input_file}. Skipping this file.")
            return
        
        # Write the header row up to and including the target column
        writer.writerow(header[:target_index + 1])
        
        # Write the data rows up to and including the target column
        for row in reader:
            writer.writerow(row[:target_index + 1])

def process_folder(input_folder, output_folder, target_column):
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    
    csv_files = glob.glob(os.path.join(input_folder, '*.csv'))
    
    for csv_file in csv_files:
        output_file = os.path.join(output_folder, os.path.basename(csv_file))
        
        # Process the file
        print(f"Processing {csv_file}...")
        delete_columns_after(csv_file, output_file, target_column)
        print(f"Saved cleaned file to {output_file}.")


process_folder(DIR_DATA_PATH, DIR_OUT, COLUMN_TARGET)
df = pd.DataFrame(columns=COLUMN_NAMES)

for root, dirs, files in os.walk(DIR_OUT):
    sample_names = list(set(file_name.replace(".csv", "").split("_")[-1] for file_name in files))
    for name in sample_names:
        matching_files = [file_name for file_name in files if name in file_name]
        with cl.ExitStack() as stack:
            file_handles = [
                stack.enter_context(open(os.path.join(root, fname), "r"))
                for fname in matching_files 
            ]
            for file in file_handles:
                df = pd.read_csv(file)
                
                # Convert 'RoiNo' to integers and 'Area' to float, then scale 'Area' by 100
                df['RoiNo'] = df['RoiNo'].astype(int)
                df['Area'] = df['Area'].astype(float) * 100
                
                # Separate luminal and arterial areas using boolean indexing
                luminal = df[df.index % 2 == 1]['Area'].tolist()  # Odd indices (1, 3, 5, ...)
                arterial = df[df.index % 2 == 0]['Area'].tolist()  # Even indices (0, 2, 4, ...)
