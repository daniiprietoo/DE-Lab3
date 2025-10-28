import os
import argparse
from glob import glob
import pandas as pd
import pydicom

# Reuse your existing logic to normalize/parse fields
from main import generate_record  # uses functions in utils.py

FLAT_COLUMNS = [
    # patient
    "patient_id",
    "patient_age",
    "patient_sex",
    # image
    "slice_thickness",
    "pixel_spacing_x",
    "pixel_spacing_y",
    "rows",
    "columns",
    "photometric_interpretation",
    # station
    "manufacturer",
    "model_name",
    # protocol
    "body_part_examined",
    "contrast_agent",
    "patient_position",
    # date
    "study_year",
    "study_month",
    # study
    "exposure_time",
    "tube_current",
    "file_path",
]


def flatten_record(rec: dict) -> dict:
    """Flatten the nested dict returned by main.generate_record into one row."""
    flat = {
        # patient
        **rec["patient"],
        # image
        **rec["image"],
        # station
        **rec["station"],
        # protocol
        **rec["protocol"],
        # date -> rename keys
        "study_year": rec["study_date"]["year"],
        "study_month": rec["study_date"]["month"],
        # study
        **rec["study"],
    }
    # Keep only expected columns in a stable order
    return {k: flat.get(k, None) for k in FLAT_COLUMNS}


def build_dicom_dataframe(data_path: str = "data/") -> pd.DataFrame:
    dicom_paths = glob(os.path.join(data_path, "*.dcm"))
    rows = []
    for fp in dicom_paths:
        try:
            ds = pydicom.dcmread(fp)
            rec = generate_record(ds)  # from main.py
            flat = flatten_record(rec)
            # ensure file_path present even if generate_record failed to set it
            flat["file_path"] = flat.get("file_path") or fp
            rows.append(flat)
        except Exception as e:
            print(f"[WARN] Skipping {fp}: {e}")
    return pd.DataFrame(rows, columns=FLAT_COLUMNS)


def main():
    parser = argparse.ArgumentParser(description="Build DICOM metadata DataFrame.")
    parser.add_argument("--data-path", default="data/", help="Folder with .dcm files")
    parser.add_argument("--out-csv", default="", help="Optional CSV output path")
    args = parser.parse_args()

    df = build_dicom_dataframe(args.data_path)
    print(f"Built DataFrame with {len(df)} rows and {len(df.columns)} columns.")
    print(df.head())

    if args.out_csv:
        df.to_csv(args.out_csv, index=False)
        print(f"Saved CSV to {args.out_csv}")


if __name__ == "__main__":
    main()
