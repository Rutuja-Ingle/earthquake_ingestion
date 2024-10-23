import json
from datetime import datetime
import requests
from google.cloud import storage
from pyspark.sql.types import IntegerType, StringType, FloatType, StructField, StructType


# Define the function to read data from GCS
def read_data_from_gcs(bucket_name, source_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)

    # Download the blob's content as text
    data = blob.download_as_text()
    print("Data from GCS:")
    print(data)  # Print the raw data to check its format
    return json.loads(data)  # Ensure this returns a dict

# -----------------------------------------------------------------------------------
# Define the function to write data to GCS
def write_data_to_gcs(bucket_name, destination_blob_name, data):
    """
    Write JSON data to GCS.

    :param bucket_name: GCS bucket name.
    :param destination_blob_name: The destination path in the bucket.
    :param data: Data to be written to GCS in JSON format.
    """
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    # Convert data to JSON string and upload
    blob.upload_from_string(data=data, content_type='application/json')
    print(f"Data written to GCS: gs://{bucket_name}/{destination_blob_name}")