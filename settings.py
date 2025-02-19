import os
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
