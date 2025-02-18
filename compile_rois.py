import pandas as pd
import os
import numpy as np
import contextlib as cl
import warnings
import natsort as ns
from itertools import zip_longest
warnings.filterwarnings("ignore", category=DeprecationWarning) 

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
    "SLICE_ID", "T1BB_ARTERIAL", "T1BB_LUMINAL", "T1BB_PLAQUE", "T1BB_CE_ARTERIAL", "T1BB_CE_LUMINAL", "T1BB_CE_PLAQUE",
    "IR_CE_ARTERIAL", "IR_CE_LUMINAL", "IR_CE_PLAQUE", "OUTCOME"
]
DIR_OUT = os.path.join("c:\\","Users\danie\Desktop\\atherosclerosis_alkystis_mri_data_analysis-main", "cleaned_data")  
COLUMN_TARGET = 'NumOfPoints'
COLUMN_MAP = {
    'T1BB': ('T1BB_ARTERIAL', 'T1BB_LUMINAL', 'T1BB_PLAQUE'),
    'T1BB_CE': ('T1BB_CE_ARTERIAL', 'T1BB_CE_LUMINAL', 'T1BB_CE_PLAQUE'),
    'IR_CE': ('IR_CE_ARTERIAL', 'IR_CE_LUMINAL', 'IR_CE_PLAQUE')
}
temp_df = pd.DataFrame(columns=[
    "SLICE_ID", 
    "T1BB_ARTERIAL", "T1BB_LUMINAL", "T1BB_PLAQUE",
    "T1BB_CE_ARTERIAL", "T1BB_CE_LUMINAL", "T1BB_CE_PLAQUE",
    "IR_CE_ARTERIAL", "IR_CE_LUMINAL", "IR_CE_PLAQUE"
])
spss_dataframe = pd.DataFrame(columns=COLUMN_NAMES)

# given a list of three files, extract the imageno range of the file of the file with lowest number of image numbers

def get_min_slices(file_paths):
    min_unique_count = float('inf')
    file_with_min_unique = None
    imageno_range = None
    for file_path in file_paths:
        try:        # Read the 'imageno' column (assuming it's the first column)
            df = pd.read_csv(file_path, usecols=[0], skiprows=1)
            # Get unique image numbers
            unique_imagenos = df.iloc[:, 0].unique()
            unique_count = len(unique_imagenos)

            # Check if this file has the fewest unique image numbers
            if unique_count < min_unique_count:
                min_unique_count = unique_count
                file_with_min_unique = file_path
                # Extract the min and max imageno values
                imageno_range = (df.iloc[:, 0].min(), df.iloc[:, 0].max())
                print("its giving", imageno_range, file_with_min_unique)
        except Exception as e:
            print(f"Error processing {file_path}: {str(e)}")
            continue
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
                df['ImageNo'] = df['ImageNo'].astype(int)
                df['RoiNo'] = df['RoiNo'].astype(int)
                df['Area'] = df['Area'].astype(float) * 100
                df = df[
                    (df["ImageNo"] >= slice_range[0]) & 
                    (df["ImageNo"] <= slice_range[1])
                ]
                unique_imagenos = df.iloc[:, 0].unique()
                unique_count = len(unique_imagenos)
                # Convert 'RoiNo' to integers and 'Area' to float, then scale 'Area' by 100
                even_roi = (df['RoiNo'] % 2 == 0) & (df['RoiNo'] >= 0)
                odd_roi = (df['RoiNo'] % 2 == 1) & (df['RoiNo'] >= 0)
                arterial = df[even_roi]['Area'].tolist() or [np.nan]
                luminal = df[odd_roi]['Area'].tolist() or [np.nan]
                plaque_area = []

                base_name = os.path.basename(file.name).replace(".csv", "")
                prefix = '_'.join(base_name.split('_')[:-1])  # Handles multi-part prefixes
                if prefix not in COLUMN_MAP:
                    continue  # Skip unknown file types
                art_col, lum_col, plaque_col = COLUMN_MAP[prefix]
                for a, l in zip(arterial, luminal):
                    try:
                        plaque_area.append(a - l)
                    except TypeError:
                        plaque_area.append(np.nan)   
                        
                slice_names = [f"{name}_S{i}" for i in unique_imagenos] 
                padded_data = list(zip_longest(slice_names, arterial, luminal, plaque_area, fillvalue=np.nan))
                file_df = pd.DataFrame(padded_data, columns=["SLICE_ID", art_col, lum_col, plaque_col])
                spss_dataframe = pd.concat([spss_dataframe, file_df], ignore_index=True)
        spss_dataframe = spss_dataframe.sort_values(by='SLICE_ID', 
                                   key=lambda x: np.argsort(ns.index_natsorted(spss_dataframe["SLICE_ID"], alg=ns.NA)))
        spss_dataframe = spss_dataframe.groupby('SLICE_ID').first().reset_index()
        spss_dataframe = spss_dataframe.loc("SLICE_ID":).astype(int)
spss_dataframe.to_csv('out.csv', index=False)  

                
                
                
