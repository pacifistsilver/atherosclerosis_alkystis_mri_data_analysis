import os, shutil, csv, glob, logging
import numpy as np
import contextlib as cl
import natsort as ns
import pandas as pd
import warnings ; warnings.warn = lambda *args,**kwargs: None
from itertools import zip_longest

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

TESTING_DATA_PATH = os.path.join("c:\\", "Users", "danie", "Desktop", "atherosclerosis_alkystis_mri_data_analysis-main", "test_data")
DATA_PATH = os.path.join("c:\\", "Users", "danie", "Desktop", "rabbit_roi_data")
OUTPUT_PATH = os.path.join("c:\\", "Users", "danie", "Desktop", "rabbit_roi_data", "cleaned_data")
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
def clean_output_path(output_folder):
    if os.path.exists(output_folder):
        shutil.rmtree(output_folder)
        logging.debug(f"Deleted existing folder: {output_folder}")
    os.makedirs(output_folder)
    logging.debug(f"Created new folder: {output_folder}")

def segment_csv(input_folder, output_folder, target_column):
    def delete_columns_after(input_file, output_file, target_column):
        with open(input_file, mode='r', newline='') as infile, open(output_file, mode='w', newline='') as outfile:
            reader = csv.reader(infile)
            writer = csv.writer(outfile)
            try:
                # Read the header row
                header = next(reader)
            except StopIteration:
                # Handle empty file
                logging.warning(f"File '{input_file}' is empty. Skipping this file.")
                return
            
            try:
                target_index = header.index(target_column)
                writer.writerow(header[:target_index + 1])
                
                for row in reader:
                    writer.writerow(row[:target_index + 1])
            except ValueError:
                logging.warning(f"Column '{target_column}' not found in {input_file}. Skipping this file.")
                return
            
    clean_output_path(output_folder)
    
    csv_files = glob.glob(os.path.join(input_folder, '*.csv'))
    
    for csv_file in csv_files:
        output_file = os.path.join(output_folder, os.path.basename(csv_file))
        
        logging.info(f"Processing {csv_file}...")
        delete_columns_after(csv_file, output_file, target_column)

def convert_to_spss_dataframe(root, files, sample_names):
    spss_dataframe = pd.DataFrame(columns=COLUMN_NAMES)
    for name in sample_names:
        matching_files = [file_name for file_name in files if name in file_name]
        with cl.ExitStack() as stack:
            
            file_handles = [stack.enter_context(open(os.path.join(root, fname), "r")) for fname in matching_files]
            
            for file in file_handles:
                df = pd.read_csv(file, usecols=["ImageNo", "RoiNo", "Area"])
                roino_counts = df.groupby('ImageNo')['RoiNo'].nunique()
                if (roino_counts == 1).any() == True: 
                    single_roino_imagenos = roino_counts[roino_counts == 1].index.tolist()
                    # Log error message if any ImageNo has only one RoiNo
                    if single_roino_imagenos:
                        for imagen_no in single_roino_imagenos:
                            logging.error(f"ImageNo {imagen_no} in {os.path.abspath(file.name)} has only one RoiNo.")
                    break
                else: 
                    logging.info(f"file check for {os.path.abspath(file.name)} OK")
                    df = df.astype({"ImageNo": int, "RoiNo": int, "Area": float})
                    df['Area'] *= 100
                    # luminal roi will always be smaller than arterial roi
                    df['shift_comparison'] = df.groupby('ImageNo')['Area'].shift(1)
                    df['shift_comparison'] = df.groupby('ImageNo')['Area'].shift(-1).fillna(df['shift_comparison'])                

                    df['luminal_roi'] = (df['Area'] < df['shift_comparison']) & (~df['shift_comparison'].isna())          
                    df['arterial_roi'] = (df['Area'] > df['shift_comparison']) & (~df['shift_comparison'].isna())            
    
                    arterial_rois = df[df['arterial_roi'] == True]['Area']
                    luminal_rois = df[df['luminal_roi'] == True]['Area'] 
                    #df = df.drop(df[df['RoiNo'] == 1].index)
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
    segment_csv(DATA_PATH, OUTPUT_PATH, COLUMN_TARGET)
    for root, dirs, files in os.walk(OUTPUT_PATH):
        sample_names = list(set(file_name.replace(".csv", "").split("_")[-1] for file_name in files))
        spss_dataframe = convert_to_spss_dataframe(root, files, sample_names)
        spss_dataframe = spss_dataframe.groupby('SLICE_ID').first().reset_index()
        spss_dataframe = spss_dataframe.sort_values(by='SLICE_ID', key=lambda x: np.argsort(ns.index_natsorted(spss_dataframe["SLICE_ID"], alg=ns.NA)))
        spss_dataframe.to_csv(os.path.join(OUTPUT_PATH, 'out.csv'), index=False)
        logging.info("Data processing complete and output saved to out.csv")

if __name__ == "__main__":
    main()
                
                
