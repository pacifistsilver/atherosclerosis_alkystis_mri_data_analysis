import pandas as pd
import os
import csv
import numpy as np
import contextlib as cl
import warnings
import natsort as ns
import glob
from itertools import zip_longest
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
warnings.filterwarnings("ignore", category=DeprecationWarning)

DIR_DATA_TEST_PATH = os.path.join("c:\\", "Users", "danie", "Desktop", "atherosclerosis_alkystis_mri_data_analysis", "test_data")
DIR_DATA_PATH = os.path.join("c:\\", "Users", "danie", "Desktop", "atherosclerosis_alkystis_mri_data_analysis", "data")
DIR_OUT = os.path.join("c:\\", "Users", "danie", "Desktop", "atherosclerosis_alkystis_mri_data_analysis", "cleaned_data")
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

def segment_csv(input_folder, output_folder, target_column):
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
            
            writer.writerow(header[:target_index + 1])
            
            for row in reader:
                writer.writerow(row[:target_index + 1])

    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    
    csv_files = glob.glob(os.path.join(input_folder, '*.csv'))
    
    for csv_file in csv_files:
        # Define the output file path
        output_file = os.path.join(output_folder, os.path.basename(csv_file))
        
        print(f"Processing {csv_file}...")
        delete_columns_after(csv_file, output_file, target_column)
        print(f"Saved cleaned file to {output_file}.")


segment_csv(DIR_DATA_PATH, DIR_OUT, COLUMN_TARGET)

def convert_to_spss_dataframe(root, files, sample_names):
    spss_dataframe = pd.DataFrame(columns=COLUMN_NAMES)
    for name in sample_names:
        matching_files = [file_name for file_name in files if name in file_name]
        with cl.ExitStack() as stack:
            
            file_handles = [stack.enter_context(open(os.path.join(root, fname), "r")) for fname in matching_files]
            slice_range = get_min_slices([os.path.abspath(file.name) for file in file_handles])
            
            for file in file_handles:
                df = pd.read_csv(file, usecols=["ImageNo", "RoiNo", "Area"])
                df.to_csv(os.path.join(DIR_OUT, 'out.csv'), index=False)
                df = df.astype({"ImageNo": int, "RoiNo": int, "Area": float})
                df['Area'] *= 100
                df = df[(df["ImageNo"] >= slice_range[0]) & (df["ImageNo"] <= slice_range[1])]
                # luminal roi will always be smaller than arterial roi
                df['shift_comparison'] = df["Area"].shift(-1)  
                df['luminal_roi'] = df['Area'] < df['shift_comparison'] 
                arterial_rois = df[df['luminal_roi'] == False]['Area']
                luminal_rois = df[df['luminal_roi'] == True]['Area']
                # area data
                # last one doesnt work rn
                arterial = arterial_rois.tolist() or [np.nan]
                luminal = luminal_rois.tolist() or [np.nan]

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
    for root, dirs, files in os.walk(DIR_OUT):
        sample_names = list(set(file_name.replace(".csv", "").split("_")[-1] for file_name in files))
        spss_dataframe = convert_to_spss_dataframe(root, files, sample_names)
        spss_dataframe = spss_dataframe.groupby('SLICE_ID').first().reset_index()
        spss_dataframe = spss_dataframe.sort_values(by='SLICE_ID', key=lambda x: np.argsort(ns.index_natsorted(spss_dataframe["SLICE_ID"], alg=ns.NA)))
        spss_dataframe.to_csv(os.path.join(DIR_OUT, 'out.csv'), index=False)
        logging.info("Data processing complete and output saved to out.csv")

if __name__ == "__main__":
    main()
                
                
