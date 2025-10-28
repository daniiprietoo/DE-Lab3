import os
import pydicom
import pandas as pd
from mongo import get_database
from glob import glob
from utils import *

DATA_PATH = "data/"  # Update this path to your DICOM files directory

def main():
    DICOM_FILES_PATH = os.path.join(DATA_PATH, "*.dcm")
    dicom_data = pd.DataFrame(
        [{"path": filepath} for filepath in glob(DICOM_FILES_PATH)]
    )
    dicom_data["file"] = dicom_data["path"].map(os.path.basename)

    print("DICOM Data Files:")
    print(dicom_data.head())

    db = get_database()

    if db is None:
        print("Database connection failed. Exiting.")
        return

    print("\nInserting DICOM file records into the database...\n")

    records = generate_records(dicom_data)

    print(f"Generated {len(records)} records from DICOM files.")
    print("Sample record:")
    print(records[0])

    dim_patient = db["dim_patient"]
    dim_image = db["dim_image"]
    dim_station = db["dim_station"]
    dim_protocol = db["dim_protocol"]
    dim_study_date = db["dim_study_date"]
    fact_study = db["fact_study"]

    for record in records:
        # Insert patient
        patient_id = get_or_create(dim_patient, record["patient"], "patient_id")
        # Insert image
        image_id = get_or_create(dim_image, record["image"], "image_id")
        # Insert station
        station_id = get_or_create(dim_station, record["station"], "station_id")
        # Insert protocol
        protocol_id = get_or_create(dim_protocol, record["protocol"], "protocol_id")
        # Insert study date
        study_date_id = get_or_create(dim_study_date, record["study_date"], "study_date_id")

        jpeg_path = dicom_to_jpeg(
            record["study"].get("file_path", ""),
            "converted_images",
            (256, 256),
        )

        if jpeg_path is None:
            print(f"Failed to convert DICOM to JPEG for study: {record['study']}")
            continue

        # Insert study
        study_values = record["study"]
        study_values.update(
            {
                "patient_id": patient_id,
                "station_id": station_id,
                "protocol_id": protocol_id,
                "study_date_id": study_date_id,
                "image_id": image_id, 
                "file_path": jpeg_path,
            }
        )
        study_id = get_or_create(fact_study, study_values, "study_id")


    print(f"Finished inserting records into the database.")

def generate_records(dicom_data: pd.DataFrame) -> list[dict[str, dict]]:
    records = []
    for _, row in dicom_data.iterrows():
        try:
            dicom_file = pydicom.dcmread(row["path"])
            record = generate_record(dicom_file)
            records.append(record)
        except Exception as e:
            print(f"Error reading {row['path']}: {e}")
    return records


def generate_record(dicom_file: pydicom.Dataset) -> dict:
    # normalize pixel spacing
    normalized_pixel_spacing = normalize_pixel_spacing(
        dicom_file.get("PixelSpacing", [])
    )
    normalized_pixel_spacing_x = normalized_pixel_spacing[0] if normalized_pixel_spacing else 0
    normalized_pixel_spacing_y = normalized_pixel_spacing[1] if normalized_pixel_spacing else 0
    normalized_contrast_agent = normalize_contrast_agent(
        dicom_file.get("ContrastBolusAgent", "")
    )

    # normalize study date
    study_year, study_month = extract_year_month(dicom_file.get("StudyDate", ""))

    # normalize patient age
    patient_age = format_age(dicom_file.get("PatientAge", ""))

    record = {
        "patient": {
            "patient_id": dicom_file.get("PatientID", ""),
            "patient_age": patient_age,
            "patient_sex": dicom_file.get("PatientSex", "Unknown"),
        },
        "image": {
            "slice_thickness": dicom_file.get("SliceThickness", ""),
            "pixel_spacing_x": (
                normalized_pixel_spacing_x
            ),
            "pixel_spacing_y": (
                normalized_pixel_spacing_y
            ),
            "rows": dicom_file.get("Rows", 0),
            "columns": dicom_file.get("Columns", 0),
            "photometric_interpretation": dicom_file.get(
                "PhotometricInterpretation", "Unknown"
            ),
        },
        "station": {
            "manufacturer": dicom_file.get("Manufacturer", ""),
            "model_name": dicom_file.get("ManufacturerModelName", ""),
        },
        "protocol": {
            "body_part_examined": dicom_file.get("BodyPartExamined", ""),
            "contrast_agent": normalized_contrast_agent,
            "patient_position": dicom_file.get("PatientPosition", ""),
        },
        "study_date": {
            "year": study_year,
            "month": study_month,
        },
        "study": {
            "exposure_time": dicom_file.get("ExposureTime", ""),
            "tube_current": dicom_file.get("XRayTubeCurrent", ""),
            "file_path": dicom_file.filename,
        },
    }
    return record


if __name__ == "__main__":
    main()


"""
patient = {
        "PatientID": surrogate key generated from relevant fields
        "PatientAge": dicom_file_metadata.PatientAge,
        "PatientSex": dicom_file_metadata.PatientSex,
    }

image = {
    "ImageID" -> surrogate key
    "SliceThickness": dicom_file_metadata.get("SliceThickness", ""),
    "PixelSpacing": dicom_file_metadata.get("PixelSpacing", ""), -> used to extract X and Y
    "PixelSpacingX": normalized first value of PixelSpacing,
    "PixelSpacingY": normalized second value of PixelSpacing,
    "Rows": dicom_file_metadata.Rows,
    "Columns": dicom_file_metadata.Columns,
    "PhotometricInterpretation": dicom_file_metadata.PhotometricInterpretation,
}

station = {
    "StationID": surrogate key generated from relevant fields
    "Manufacturer": dicom_file_metadata.get("Manufacturer", ""),
    "ModelName": dicom_file_metadata.get("ManufacturerModelName", ""),
}

protocol = {
    "ProtocolID" -> 
    "BodyPartExamined": dicom_file_metadata.get("BodyPartExamined", ""),
    "ContrastAgent": dicom_file_metadata.get("ContrastBolusAgent", ""),
    "PatientPosition": dicom_file_metadata.get("PatientPosition", ""),
}

date = {
    "DateID":  surrogate key generated from relevant fields
    "StudyDate": dicom_file_metadata.get("StudyDate", ""), -> used to extract Year and Month
    "Year": extracted from StudyDate,
    "Month": extracted from StudyDate
}

study ={
    "StudyID": surrogate key generated from relevant fields
    "PatientID": patient["PatientID"],
    "StationID": station["StationID"],
    "ProtocolID": protocol["ProtocolID"],
    "DateID": date["DateID"],
    "ExposureTime": dicom_file_metadata.get("ExposureTime", ""),
    "TubeCurrent": dicom_file_metadata.get("TubeCurrent", ""),
    "FilePath": path to the jpeg image converted from DICOM
}

"""
