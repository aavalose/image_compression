import numpy as np
import datetime
import requests
from PIL import Image
from io import BytesIO
import json
import traceback

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from pymongo import MongoClient
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import udf
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# This file aims to download data from MongoDB, compress it using SVD, and upload the images to the Google Cloud storage bucket.

# Compressing functions
def url_to_pixel_matrix(image_url):
    """
    Converts an image URL into a pixel matrix representation.
    
    Args:
        image_url (str): URL of the image to convert
        
    Returns:
        numpy.ndarray: 3D array of pixel values with shape (height, width, channels)
                      Values range from 0-255 for each RGB channel
    
    Raises:
        requests.RequestException: If there is an error downloading the image
        PIL.UnidentifiedImageError: If the downloaded data is not a valid image
    """
    # Download image from URL
    response = requests.get(image_url)
    response.raise_for_status()
    
    # Convert to PIL Image
    img = Image.open(BytesIO(response.content))
    
    # Convert to RGB mode if not already
    if img.mode != 'RGB':
        img = img.convert('RGB')
    
    # Convert to numpy array
    pixel_matrix = np.array(img)
    
    return pixel_matrix

def compress_image(pixel_matrix, k=100):
    """
    Compresses an image represented by a pixel matrix using SVD decomposition.
    
    Args:
        pixel_matrix (numpy.ndarray): 3D array of pixel values with shape (height, width, channels)
        k (int): Number of singular values to keep in the SVD decomposition. Controls compression level.
                Higher values preserve more detail but increase size.

    Returns:
        numpy.ndarray: Compressed pixel matrix with shape (height, width, channels)
        int: Number of singular values used in the compression
    """
    # Handle each color channel separately
    height, width, channels = pixel_matrix.shape
    compressed_matrix = np.zeros((height, width, channels))
    
    for channel in range(channels):
        # Perform SVD decomposition on each channel
        U, S, V = np.linalg.svd(pixel_matrix[:, :, channel])
        
        # Reconstruct using top k singular values
        compressed_channel = np.dot(U[:, :k], np.dot(np.diag(S[:k]), V[:k, :]))
        compressed_matrix[:, :, channel] = compressed_channel
    
    # Ensure pixel values stay in valid range
    compressed_matrix = np.clip(compressed_matrix, 0, 255)
    
    return compressed_matrix.astype(np.uint8)

# Task 1: Download data from MongoDB:
def extract_data_from_mongodb(**kwargs):
    """
    Extract data from MongoDB, convert it to a JSON string, and push it to XCom.
    
    Hardcoded values:
      - mongo_conn_id: 'mongo_connect'
      - database: 'images'
      - collection: 'raw_data'
      
    Expected kwargs:
      - ti: Airflow task instance (for XCom operations)
      - xcom_key: Optional; the key to push the JSON content to (default: 'mongodb_data_json')
    """
    try:
        ti = kwargs['ti']
        xcom_key = kwargs.get('xcom_key', 'mongodb_data_json')
        
        # Connect to MongoDB using a hardcoded connection id
        hook = MongoHook(mongo_conn_id='mongo_connect')
        client = hook.get_conn()
        db = client.images
        collection = db.image_sized

        # Extract all documents from the collection
        cursor = collection.find({})
        data = list(cursor)
        
        # Convert ObjectIDs to strings to avoid serialization issues
        for doc in data:
            if '_id' in doc:
                doc['_id'] = str(doc['_id'])
        
        # Convert the extracted data to a JSON string
        json_content = json.dumps(data)
        
        # Push the JSON content to XCom so that downstream tasks can use it
        ti.xcom_push(key=xcom_key, value=json_content)
        print("Successfully extracted data from MongoDB and pushed JSON to XCom.")

    except Exception as e:
        print(f"Error extracting data from MongoDB: {e}")
        print(traceback.format_exc())

# Task 2: Compress and upload images and metadata to GCS
# Uploading to GCS functions
def upload_image_to_gcp(compressed_image, folder, file_name):
    """
    Converts the compressed image (numpy array) to PNG and uploads it to GCS.
    """
    # Convert numpy array back to a PIL Image and save to a bytes buffer
    img = Image.fromarray(compressed_image)
    buffer = BytesIO()
    img.save(buffer, format='PNG')
    image_data = buffer.getvalue()
    
    # Hardcoded GCS details
    gcs_conn_id = "google_cloud_datastore_default"
    bucket_name = "us-west1-msds-697-spring25-ad143b43-bucket"
    object_name = f"data/compressed_images/{folder}/{file_name}.png"
    
    gcs_hook = GCSHook(gcp_conn_id=gcs_conn_id)
    gcs_hook.upload(
        bucket_name=bucket_name,
        object_name=object_name,
        data=image_data,
        mime_type='image/png'
    )
    print(f"Uploaded image to gs://{bucket_name}/{object_name}")

def upload_metadata_to_gcp(metadata, folder, file_name):
    """
    Converts metadata to JSON and uploads it to GCS.
    """
    json_content = json.dumps(metadata)
    gcs_conn_id = "google_cloud_datastore_default"
    bucket_name = "us-west1-msds-697-spring25-ad143b43-bucket"
    object_name = f"data/compressed_images/{folder}/{file_name}.json"
    
    gcs_hook = GCSHook(gcp_conn_id=gcs_conn_id)
    gcs_hook.upload(
        bucket_name=bucket_name,
        object_name=object_name,
        data=json_content,
        mime_type='application/json'
    )
    print(f"Uploaded metadata to gs://{bucket_name}/{object_name}")

def process_and_upload_images(**kwargs):
    """
    Process the MongoDB data directly from XCom, download each image using its URL,
    compress it using SVD, and upload both the compressed image and its metadata to GCS.
    
    Expected kwargs:
      - source_task: The task ID from which to pull the JSON metadata.
      - xcom_key: Optional; the XCom key used for the JSON metadata (default: 'mongodb_data_json').
    """
    try:
        ti = kwargs['ti']
        xcom_key = kwargs.get('xcom_key', 'mongodb_data_json')
        source_task = kwargs.get('source_task')
        if not source_task:
            raise ValueError("source_task must be provided to pull the file content from XCom.")

        json_metadata = ti.xcom_pull(key=xcom_key, task_ids=source_task)
        if not json_metadata:
            raise ValueError("No metadata found in XCom.")
        data = json.loads(json_metadata)

        for doc in data:
            try:
                # Retrieve image URL from the 'url' field.
                image_url = doc.get("url")
                if not image_url:
                    print(f"No image URL found for document with id {doc.get('id', 'unknown')}. Skipping.")
                    continue
                
                # Download and compress the image
                pixel_matrix = url_to_pixel_matrix(image_url)
                compressed = compress_image(pixel_matrix, k=100)
                
                # Use the "photo_size" field directly for folder name.
                folder = doc.get("photo_size", "unknown")
                
                # Use the 'id' field for the file name
                file_name = str(doc.get("title", "image"))
                
                # Upload compressed image and metadata to GCP
                upload_image_to_gcp(compressed, folder, file_name)
                upload_metadata_to_gcp(doc, folder, file_name)
                print(f"Processed and uploaded image {file_name} to folder {folder}")
            except Exception as inner_e:
                print(f"Error processing image with id {doc.get('id', 'unknown')}: {inner_e}")
                traceback.print_exc()
    except Exception as e:
        print(f"Error in process_and_upload_images: {e}")
        traceback.print_exc()

# DAG
with DAG(
    dag_id="download_compress_upload",
    start_date=datetime.datetime.now(),  # Start today
    end_date=datetime.datetime.now() + datetime.timedelta(days=3),  # End in 3 days
    schedule_interval="0 0,12 * * *",  # Twice a day: midnight and noon
    catchup=False,
    max_active_runs=1,
    description="Extract metadata from MongoDB, process images, and upload compressed images and metadata to GCP."
):
    
    # Task to extract data from MongoDB
    extract_task = PythonOperator(
        task_id='extract_data_from_mongodb',
        python_callable=extract_data_from_mongodb,
        provide_context=True
    )
    
    # Task 2: Process images using the metadata from XCom and upload to GCP
    process_upload_task = PythonOperator(
        task_id='process_and_upload_images',
        python_callable=process_and_upload_images,
        op_kwargs={'source_task': 'extract_data_from_mongodb'},
        provide_context=True
    )
    
    # Set task dependencies: extract_task >> process_upload_task
    extract_task >> process_upload_task
