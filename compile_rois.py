import pandas as pd
import os
import numpy as np
import contextlib as cl
import warnings
import natsort as ns
from itertools import zip_longest
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
warnings.filterwarnings("ignore", category=DeprecationWarning)

# Constants
DIR_DATA_TEST_PATH = os.path.join("c:\\", "Users", "danie", "Desktop", "atherosclerosis_alkystis_mri_data_analysis-main", "test_data")
DIR_DATA_PATH = os.path.join("c:\\", "Users", "danie", "Desktop", "atherosclerosis_alkystis_mri_data_analysis-main", "cleaned_data")
DIR_OUT = os.path.join("c:\\", "Users", "danie", "Desktop", "atherosclerosis_alkystis_mri_data_analysis-main", "cleaned_data")
COLUMN_NAMES = [
    "SLICE_ID", "T1BB_ARTERIAL", "T1BB_LUMINAL", "T1BB_PLAQUE", "T1BB_CE_ARTERIAL", "T1BB_CE_LUMINAL", "T1BB_CE_PLAQUE",
    "IR_CE_ARTERIAL", "IR_CE_LUMINAL", "IR_CE_PLAQUE", "OUTCOME"
]
COLUMN_TARGET = 'NumOfPoints'
COLUMN_MAP = {
    'T1BB': ('T1BB_ARTERIAL', 'T1BB_LUMINAL', 'T1BB_PLAQUE'),
    'T1BB_CE': ('T1BB_CE_ARTERIAL', 'T1BB_CE_LUMINAL', 'T1BB_CE_PLAQUE'),
    'IR_CE': ('IR_CE_ARTERIAL', 'IR_CE_LUMINAL', 'IR_CE_PLAQUE')
}

def get_min_slices(file_paths):
    """Determine the file with the minimum number of unique image numbers and return its range."""
    min_unique_count = float('inf')
    file_with_min_unique = None
    imageno_range = None
    for file_path in file_paths:
        try:
            df = pd.read_csv(file_path, usecols=[0], skiprows=1)
            unique_imagenos = df.iloc[:, 0].unique()
            unique_count = len(unique_imagenos)
            if unique_count < min_unique_count:
                min_unique_count = unique_count
                file_with_min_unique = file_path
                imageno_range = (df.iloc[:, 0].min(), df.iloc[:, 0].max())
        except Exception as e:
            logging.error(f"Error processing {file_path}: {str(e)}")
            continue
    return imageno_range

def process_files(root, files, sample_names):
    """Process each file to extract and compile data into a DataFrame."""
    spss_dataframe = pd.DataFrame(columns=COLUMN_NAMES)
    for name in sample_names:
        matching_files = [file_name for file_name in files if name in file_name]
        with cl.ExitStack() as stack:
            
            file_handles = [stack.enter_context(open(os.path.join(root, fname), "r")) for fname in matching_files]
            slice_range = get_min_slices([os.path.abspath(file.name) for file in file_handles])
            
            for file in file_handles:
                df = pd.read_csv(file, usecols=["ImageNo", "RoiNo", "Area"])
                df = df.astype({"ImageNo": int, "RoiNo": int, "Area": float})
                df['Area'] *= 100
                df = df[(df["ImageNo"] >= slice_range[0]) & (df["ImageNo"] <= slice_range[1])]
                # even roi = arterial, odd roi = luminal
                even_roi = (df['RoiNo'] % 2 == 0) & (df['RoiNo'] >= 0)
                odd_roi = (df['RoiNo'] % 2 == 1) & (df['RoiNo'] >= 0)
                
                # area data
                arterial = df[even_roi]['Area'].tolist() or [np.nan]
                luminal = df[odd_roi]['Area'].tolist() or [np.nan]
                plaque_area = [a - l if None not in (a, l) else np.nan for a, l in zip(arterial, luminal)]
                
                base_name = os.path.basename(file.name).replace(".csv", "")
                prefix = '_'.join(base_name.split('_')[:-1])
                
                if prefix in COLUMN_MAP:
                    # map file prefix to column names
                    art_col, lum_col, plaque_col = COLUMN_MAP[prefix]
                    slice_names = [f"{name}_S{i}" for i in df['ImageNo'].unique()]
                    padded_data = list(zip_longest(slice_names, arterial, luminal, plaque_area, fillvalue=np.nan))
                    file_df = pd.DataFrame(padded_data, columns=["SLICE_ID", art_col, lum_col, plaque_col])
                    spss_dataframe = pd.concat([spss_dataframe, file_df], ignore_index=True)
                    
    return spss_dataframe

def main():
    """Main function to orchestrate the data processing."""
    for root, dirs, files in os.walk(DIR_DATA_PATH):
        sample_names = list(set(file_name.replace(".csv", "").split("_")[-1] for file_name in files))
        spss_dataframe = process_files(root, files, sample_names)
        spss_dataframe = spss_dataframe.groupby('SLICE_ID').first().reset_index()
        spss_dataframe = spss_dataframe.sort_values(by='SLICE_ID', key=lambda x: np.argsort(ns.index_natsorted(spss_dataframe["SLICE_ID"], alg=ns.NA)))
        spss_dataframe.to_csv(os.path.join(DIR_OUT, 'out.csv'), index=False)
        logging.info("Data processing complete and output saved to out.csv")

if __name__ == "__main__":
    main()
                
                
