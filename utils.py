import os
from PIL import Image
import numpy as np
from pymongo.collection import Collection
import pydicom
import hashlib
import json

def surrogate_key(values: dict) -> str:
    clean_values = {k: str(v) if v is not None else "NULL" for k, v in values.items()}
    hash_input = json.dumps(clean_values, sort_keys=True)
    return hashlib.md5(hash_input.encode("utf-8")).hexdigest()


def get_or_create(collection: Collection, values: dict, pk_name: str):
    # Generate surrogate key
    sk = surrogate_key(values)
    # Check if document with this surrogate key already exists
    existing_doc = collection.find_one({pk_name: sk})

    if existing_doc:
        print(f"[MATCH] Found existing document for {pk_name}: {sk}")
        return existing_doc[pk_name]
    # If not, insert new document
    values[pk_name] = sk
    collection.insert_one(values)
    print(f"[INSERT] Inserted new document for {pk_name}: {sk}")
    return sk


def format_age(age_str: str) -> int | None:
    """Convert DICOM age ('045Y') to integer years."""
    if not age_str or len(age_str) != 4:
        print(f"Invalid age string: {age_str}")
        return None
    return int(age_str[:3])


def normalize_contrast_agent(raw_value):
    if not raw_value or raw_value == "":
        return "No contrast agent"

    if len(raw_value) == 1:
        return "No contrast agent"

    return raw_value.lower().strip()


def dicom_to_jpeg(input_path, output_dir, size):
    try:
        os.makedirs(output_dir, exist_ok=True)
        dicom_data = pydicom.dcmread(input_path)
        pixel_array = dicom_data.pixel_array

        pixel_array = pixel_array.astype(float)
        min_pixel = pixel_array.min()
        max_pixel = pixel_array.max()

        if max_pixel > min_pixel:
            scaled_pixel_array = (
                (pixel_array - min_pixel) / (max_pixel - min_pixel)
            ) * 255.0
        else:
            scaled_pixel_array = np.zeros_like(pixel_array)

        image = Image.fromarray(scaled_pixel_array.astype(np.uint8), mode="L")
        image = image.resize(size, Image.LANCZOS)
        output_path = os.path.join(
            output_dir, os.path.basename(input_path).replace(".dcm", ".jpeg")
        )
        image.save(output_path, "JPEG")

        return output_path

    except Exception as e:
        print(f"Error processing {input_path}: {e}")
        return None


def round_to_nearest_bin(value, bins):
    """Round a value to the nearest bin."""
    return min(bins, key=lambda x: abs(x - value))


def normalize_pixel_spacing(raw_value):
    """Convert DICOM PixelSpacing to a list of floats."""
    if not raw_value:
        return None
    normalized = [float(v) for v in raw_value]
    bins = [0.6, 0.65, 0.7, 0.75, 0.8]
    return [round_to_nearest_bin(v, bins) for v in normalized]

def extract_year_month(date_string: str):
    if len(date_string) < 6:
        return None, None
    year = int(date_string[:4])
    month = int(date_string[4:6])
    return year, month
