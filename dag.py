import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from compression import compress_image, url_to_pixel_matrix
from call_retrievers import get_pexels_images, get_wallhaven_photos, get_unsplash_images

def process_images(get_images_func, **context):
    """Get images from API and return URLs and metadata"""
    urls, metadata = get_images_func(limit=5)
    return {'urls': urls, 'metadata': metadata}

def compress_images(ti, **context):
    """Compress images from previous task"""
    task_data = ti.xcom_pull(task_ids=context['task'].upstream_task_ids.pop())
    urls = task_data['urls']
    
    compressed_images = []
    for url in urls:
        pixel_matrix = url_to_pixel_matrix(url)
        compressed = compress_image(pixel_matrix, k=50)
        compressed_images.append(compressed)
    
    return compressed_images

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
    
    wallhaven_start = PythonOperator(
        task_id="wallhaven_start", 
        python_callable=process_images,
        op_kwargs={'get_images_func': get_wallhaven_photos}
    )
    
    unsplash_start = PythonOperator(
        task_id="unsplash_start",
        python_callable=process_images,
        op_kwargs={'get_images_func': get_unsplash_images}
    )

    # Compression tasks
    pexels_compress = PythonOperator(
        task_id="pexels_compress",
        python_callable=compress_images
    )
    
    wallhaven_compress = PythonOperator(
        task_id="wallhaven_compress",
        python_callable=compress_images
    )
    
    unsplash_compress = PythonOperator(
        task_id="unsplash_compress",
        python_callable=compress_images
    )
    
    # Define task dependencies
    pexels_start >> pexels_compress
    wallhaven_start >> wallhaven_compress 
    unsplash_start >> unsplash_compress
