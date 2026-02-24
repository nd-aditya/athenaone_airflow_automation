import os
from google.cloud import storage
from .base import CloudClient
from deIdentification.nd_logger import nd_logger


class GCPClient(CloudClient):
    def __init__(self):
        """Initialize the GCP storage client"""
        self.bucket = os.environ.get("CLOUD_STORAGE_BUCKET_NAME", None)
        self.client = storage.Client()
        self.bucket = self.client.bucket(self.bucket)

    def download_file(self, cloud_file_path):
        """Returns file data of object_key from bucket_name"""
        blob = self.bucket.blob(cloud_file_path)
        if not blob.exists():
            raise FileNotFoundError(f"File {cloud_file_path} not found in bucket {self.bucket}.")
        return blob.download_as_bytes()

    def upload_file(self, cloud_file_path, data, content_type="application/octet-stream"):
        """Uploads file as object_key in bucket_name"""
        blob = self.bucket.blob(cloud_file_path)
        blob.upload_from_string(data, content_type=content_type)
        nd_logger.info(f"File {cloud_file_path} uploaded successfully to {self.bucket}.")

    def upload_file_from_fs(self, cloud_file_path, full_path_to_file, content_type="application/octet-stream"):
        """Uploads local file with file_path as object_key in bucket_name"""
        blob = self.bucket.blob(cloud_file_path)
        blob.upload_from_filename(full_path_to_file, content_type=content_type)
        nd_logger.info(f"File {full_path_to_file} uploaded as {cloud_file_path} in bucket {self.bucket}.")

    def exists(self, cloud_file_path):
        """Returns true if file exists"""
        blob = self.bucket.blob(cloud_file_path)
        return blob.exists()

    def delete_file(self, cloud_file_path):
        """Deletes the specified file"""
        blob = self.bucket.blob(cloud_file_path)
        if blob.exists():
            blob.delete()
            nd_logger.info(f"File {cloud_file_path} deleted from {self.bucket}.")
        else:
            raise FileNotFoundError(f"File {cloud_file_path} not found in bucket {self.bucket}.")
