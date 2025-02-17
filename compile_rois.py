import csv
import statistics as stats
import pandas as pd
import ntpath
import os
import glob
import contextlib as cl
import re
from collections import defaultdict

# extract Area column for RoiNo 0 and RoiNo 1 
# calculate mean Are a for RoiNo 0 and RoiNo 1
# print value to new excel file 
FILE_PATH="C:/Users/danie/Desktop/test_rabbit_mri_data/data"
filename=f"{FILE_PATH}/IR_CE_DALE2.csv"
data=[]
# get corresponding row value from RoiNo
# eg. first 0 has row number of 2. get 2 row number then get Area in same row
# two rois for each image = all luminal areas will have an odd numbered index
# bitwise and operator is fastest vs %
# write: filename, mean luminal, mean arterial, vessel mean.
directory = os.path.join("c:\\","Users\danie\Desktop\\test_rabbit_mri_data\data")
column_names = [
    "SLICE_NR", "T1BB_OUTER", "T1BB_INNER", "T1BB_PLAQUE", "T1BB_CE_OUTER", "T1BB_CE_INNER", "T1BB_CE_PLAQUE",
    "IR_CE_OUTER", "IR_CE_INNER", "IR_CE_PLAQUE", "OUTCOME"
]

input_folder = directory  # Replace with the path to your input folder
output_folder = os.path.join(directory, "cleaned_data")  # Replace with the path to your output folder
target_column = 'NumOfPoints'  # Replace with the target column name
def delete_columns_after(input_file, output_file, target_column):
    """
    Delete all columns after a specific column in a CSV file.
    """
    with open(input_file, mode='r', newline='') as infile, open(output_file, mode='w', newline='') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        
        # Read the header row
        header = next(reader)
        
        # Find the index of the target column
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
    """
    Process all CSV files in a folder and delete columns after the target column.
    """
    # Create the output folder if it doesn't exist
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    
    # Get all CSV files in the input folder
    csv_files = glob.glob(os.path.join(input_folder, '*.csv'))
    
    # Process each CSV file
    for csv_file in csv_files:
        # Define the output file path
        output_file = os.path.join(output_folder, os.path.basename(csv_file))
        
        # Process the file
        print(f"Processing {csv_file}...")
        delete_columns_after(csv_file, output_file, target_column)
        print(f"Saved cleaned file to {output_file}.")


process_folder(input_folder, output_folder, target_column)
# Create an empty DataFrame with the specified column names
df = pd.DataFrame(columns=column_names)
print(df)
for root, dirs, files in os.walk(output_folder):
    sample_names = list(set(filename.replace(".csv", "").split("_")[-1] for filename in files))
    for name in sample_names:
        matching_files = [filename for filename in files if name in filename]
        with cl.ExitStack() as stack:
            file_handles = [
                stack.enter_context(open(os.path.join(root, fname), "r"))
                for fname in matching_files 
            ]
            for file in file_handles:
                # Read the CSV file directly into a Pandas DataFrame
                df = pd.read_csv(file)
                
                # Convert 'RoiNo' to integers and 'Area' to float, then scale 'Area' by 100
                df['RoiNo'] = df['RoiNo'].astype(int)
                df['Area'] = df['Area'].astype(float) * 100
                
                # Separate luminal and arterial areas using boolean indexing
                luminal = df[df.index % 2 == 1]['Area'].tolist()  # Odd indices (1, 3, 5, ...)
                arterial = df[df.index % 2 == 0]['Area'].tolist()  # Even indices (0, 2, 4, ...)

# transform df to add to old df
# add slice no for each slice look at image number



"""     luminal arterial plaque ... outcome.
SAMPLE  
s1
s2
...

"""

# df.to_csv("roi_data.csv", sep=',', encoding='utf-8', mode="w")


# each slice should be coded with a name e.g. BILLY_S1, BILLY_S2, etc. 
# not all samples have the same number of slices... 
# write all slices and replace values if value NA

# read in batches e.g. DALE2 batch > FRANK batch etc.. 
# read in groups of 3
# extracting the same information as last time except repeat for each file type 


