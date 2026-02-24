import os
import hashlib
import pandas as pd
from google.cloud import storage

def calculate_checksum(file_path, algorithm="md5"):
    hash_func = hashlib.new(algorithm)
    try:
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_func.update(chunk)
        return hash_func.hexdigest()
    except Exception as e:
        return str(e)

# Set up Google Cloud Storage client
def upload_to_gcs(bucket_name, source_folder, destination_folder, date_folder):
    file_data = []

    # Initialize a GCS client
    client = storage.Client()
   
    # Get the GCS bucket
    bucket = client.bucket(bucket_name)
   
    # Walk through the source folder and upload files
    for root, dirs, files in os.walk(source_folder):
        for file_name in files:
            if file_name.endswith(".sql"):
                file_path = os.path.join(root, file_name)
                file_size_bytes = os.path.getsize(file_path)
                checksum = calculate_checksum(file_path)
                file_size_kb = file_size_bytes / 1024
                file_size_mb = file_size_bytes / (1024 * 1024)
                file_data.append({"file_path": file_path, "checksum": checksum, "file_size_bytes": file_size_bytes, "file_size_kb": round(file_size_kb, 2), "file_size_mb": round(file_size_mb, 2)})

                # Construct the destination path in GCS
                destination_blob_name = os.path.join(destination_folder, os.path.relpath(file_path, source_folder)).replace("\\", "/")
            
                # Upload file to GCS
                blob = bucket.blob(destination_blob_name)
                blob.upload_from_filename(file_path)
                print(f'File {file_path} uploaded to {destination_blob_name}.')
    
    df = pd.DataFrame(file_data)
    df.to_csv(f"{date_folder}/sql_dump_stats.csv", index=False)
    blob = bucket.blob(fr'tables/batch2/{date_folder}/sql_dump_stats.csv')
    blob.upload_from_filename(fr'D:\ssuman\GCP Transfer\{date_folder}\sql_dump_stats.csv')

if __name__ == "__main__":
    # Define your bucket name
    BUCKET_NAME = 'nd-platform-dent-dump'
    date_folder = "05022025"
    
    # The local folder you want to upload
    SOURCE_FOLDER = fr'D:\ssuman\GCP Transfer\{date_folder}'  # Removed trailing backslash
    
    # The GCS destination folder (optional)
    DESTINATION_FOLDER = fr'tables\batch2\{date_folder}'  # Use '' if you want to upload to root of bucket
   
    # Upload the folder
    upload_to_gcs(BUCKET_NAME, SOURCE_FOLDER, DESTINATION_FOLDER, date_folder)