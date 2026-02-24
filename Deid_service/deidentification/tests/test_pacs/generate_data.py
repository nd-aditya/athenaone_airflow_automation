import os
import pydicom
from pydicom.uid import generate_uid

def generate_sample_data(input_file, output_dir, patient_count=10):
    if not os.path.isfile(input_file):
        raise FileNotFoundError(f"Input file not found: {input_file}")
    os.makedirs(output_dir, exist_ok=True)

    for p_idx in range(1, patient_count + 1):
        patient_id = str(p_idx)  # Numeric value, but stored as string per DICOM standard
        patient_dir = os.path.join(output_dir, f"PATIENT_{p_idx:03d}")
        os.makedirs(patient_dir, exist_ok=True)

        for s_idx in range(1, 4):  # Up to 3 studies
            study_uid = generate_uid()
            study_dir = os.path.join(patient_dir, f"STUDY_{s_idx:02d}")
            os.makedirs(study_dir, exist_ok=True)

            series_uid = generate_uid()
            series_dir = os.path.join(study_dir, "SERIES_01")  # 1 series per study
            os.makedirs(series_dir, exist_ok=True)

            for i_idx in range(1, 3):  # Up to 2 instances per series
                ds = pydicom.dcmread(input_file)
                ds.PatientID = patient_id  # Numeric string
                ds.StudyInstanceUID = study_uid
                ds.SeriesInstanceUID = series_uid
                ds.SOPInstanceUID = generate_uid()
                ds.PatientName = f"Sample^Patient{p_idx}"
                ds.StudyID = f"{p_idx:03d}{s_idx:02d}"
                ds.SeriesNumber = 1
                ds.InstanceNumber = i_idx

                output_path = os.path.join(series_dir, f"IMG_{i_idx:03d}.dcm")
                ds.save_as(output_path)

input_dicom_path = "/Users/rohitchouhan/Documents/Code/backend/deidentification/tests/assets/dicoms/dicom1.dcm"
output_dir = "/Users/rohitchouhan/Documents/Code/backend/deidentification/tests/assets/dicoms/sampledata"

generate_sample_data(input_dicom_path, output_dir)
