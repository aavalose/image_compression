import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from compression import compress_image, url_to_pixel_matrix
from call_retrievers import get_pexels_images, get_wallhaven_photos, get_unsplash_images
from pymongo import MongoClient

def process_images(get_images_func, **context):
    """Get images from API and return URLs and metadata"""
    urls, metadata = get_images_func(limit=5)
    return {'urls': urls, 'metadata': metadata}

def push_to_mongodb(data, **context):
    """Push image data to MongoDB"""
    client = MongoClient('mongodb://localhost:27017/')
    db = client['image_database']
    collection = db['image_metadata']
    collection.insert_many(data)
    client.close()

with DAG(
    dag_id="images_compression",
    start_date=datetime.datetime(2025, 2, 22),
    schedule="@daily",
    catchup=False,
    max_active_runs=1
):
    # Image retrieval tasks
    pexels_start = PythonOperator(
        task_id="pexels_start",
        python_callable=process_images,
        op_kwargs={'get_images_func': get_pexels_images}
    )
    
    unsplash_start = PythonOperator(
        task_id="unsplash_start",
        python_callable=process_images,
        op_kwargs={'get_images_func': get_unsplash_images}
    )

    # Task to push Pexels data to MongoDB
    pexels_to_mongo = PythonOperator(
        task_id="pexels_to_mongo",
        python_callable=push_to_mongodb,
        op_kwargs={'data': "{{ task_instance.xcom_pull(task_ids='pexels_start') }}"}
    )

    # Task to push Unsplash data to MongoDB
    unsplash_to_mongo = PythonOperator(
        task_id="unsplash_to_mongo",
        python_callable=push_to_mongodb,
        op_kwargs={'data': "{{ task_instance.xcom_pull(task_ids='unsplash_start') }}"}
    )

    # Set task dependencies
    pexels_start >> pexels_to_mongo
    unsplash_start >> unsplash_to_mongo