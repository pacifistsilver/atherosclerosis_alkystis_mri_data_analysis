import csv
import statistics as stats
import pandas as pd
import ntpath
import os
import glob
import contextlib as cl
import re
from collections import defaultdict

"""
Example DataFrame structure:
     luminal  arterial  plaque  ...  outcome
SAMPLE  
s1
s2
...
"""
DIR_DATA_TEST_PATH= os.path.join("c:\\","Users\danie\Desktop\\atherosclerosis_alkystis_mri_data_analysis-main", "test_data")
DIR_DATA_PATH = os.path.join("c:\\","Users\danie\Desktop\\atherosclerosis_alkystis_mri_data_analysis-main\data")
COLUMN_NAMES = [
    "SLICE_NR", "T1BB_ARTERIAL", "T1BB_LUMINAL", "T1BB_PLAQUE", "T1BB_CE_ARTERIAL", "T1BB_CE_LUMINAL", "T1BB_CE_PLAQUE",
    "IR_CE_ARTERIAL", "IR_CE_LUMINAL", "IR_CE_PLAQUE", "OUTCOME"
]
DIR_OUT = os.path.join("c:\\","Users\danie\Desktop\\atherosclerosis_alkystis_mri_data_analysis-main", "cleaned_data")  # Replace with the path to your output folder
COLUMN_TARGET = 'NumOfPoints'  # Replace with the target column name
spss_dataframe = pd.DataFrame(columns=COLUMN_NAMES)

# given a list of three files, extract the imageno range of the file of the file with lowest number of image numbers

def get_min_slices(file_paths):
    min_unique_count = float('inf')
    file_with_min_unique = None
    imageno_range = None
    for file_path in file_paths:
        # Read the 'imageno' column (assuming it's the first column)
        df = pd.read_csv(file_path, usecols=[0])
        
        # Get unique image numbers
        unique_imagenos = df.iloc[:, 0].unique()
        unique_count = len(unique_imagenos)
        
        # Check if this file has the fewest unique image numbers
        if unique_count < min_unique_count:
            min_unique_count = unique_count
            file_with_min_unique = file_path
            # Extract the min and max imageno values
            imageno_range = (df.iloc[:, 0].min(), df.iloc[:, 0].max())
    return imageno_range

for root, dirs, files in os.walk(DIR_DATA_TEST_PATH):
    sample_names = list(set(file_name.replace(".csv", "").split("_")[-1] for file_name in files))
    for name in sample_names:
        matching_files = [file_name for file_name in files if name in file_name]
        with cl.ExitStack() as stack:
            file_handles = [
                stack.enter_context(open(os.path.join(root, fname), "r"))
                for fname in matching_files 
            ]
            slice_range = get_min_slices([os.path.abspath(file.name) for file in file_handles])
            for file in file_handles:
                df = pd.read_csv(file,usecols=["ImageNo","RoiNo","Area"])
                df = df[(df.iloc[:, 0] >= slice_range[0]) & (df.iloc[:, 0] <= slice_range[1])]
                # Convert 'RoiNo' to integers and 'Area' to float, then scale 'Area' by 100
                df['RoiNo'] = df['RoiNo'].astype(int)
                df['Area'] = df['Area'].astype(float) * 100
                
                # Separate luminal and arterial areas using boolean indexing
                luminal = df[df['RoiNo'] % 2 == 1]['Area'].tolist()  # Odd indices (1, 3, 5, ...)
                arterial = df[df['RoiNo'] % 2 == 0]['Area'].tolist()  # Even indices (0, 2, 4, ...)
                plaque_area = [x - y for x, y in zip(arterial, luminal)]
                
# transforming data
# transform the dataframe df to containing columns referenced in the name of the csv file
# add columns [CASE_NAME_LUMINAL, CASE_NAME_LUMINAL, CASE_NAME_PLAQUE] 
# extract name of the case from the original file. 
# populate spss_dataframe
                
                
                
                
