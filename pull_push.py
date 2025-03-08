import datetime
import requests
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from pymongo import MongoClient
from airflow.providers.mongo.hooks.mongo import MongoHook
import traceback

def push_to_mongodb(**kwargs):
    """Push image data to MongoDB"""
    try:
        task_instance = kwargs['ti']  # Get task instance
        results = task_instance.xcom_pull(task_ids=kwargs['task_ids'])  # Fetch XCom data

        if not results or not isinstance(results, dict):  
            raise ValueError("No valid data received from XCom")

        hook = MongoHook(mongo_conn_id='mongo_connect')
        client = hook.get_conn()

        db = client.images
        collection = db.raw_data
        print(f"Connected to MongoDB - {client.server_info()}")
        print(results)
        
        collection.insert_many(results['metadata'])  # Insert metadata (adjust as needed)
    except Exception as e:
        print(f"Error connecting to MongoDB -- {e}")
        print(traceback.format_exc())

def get_pexels_images(query=None, limit=10, photo_id=None, photographer=None):
    """
    Retrieves images from the Pexels API based on provided parameters.

    Args:
        query (str, optional): Search term for photos. Required if photo_id not provided.
        limit (int, optional): Maximum number of photos to return. Defaults to 10.
        photo_id (str, optional): Specific Pexels photo ID to retrieve. If provided, query is ignored.
        photographer (str, optional): Filter results by photographer name.

    Returns:
        tuple: A tuple containing two lists:
            - List of URLs for original size photos
            - List of complete photo metadata dictionaries from Pexels API

    Examples:
        # Search for up to 10 mountain photos
        urls, metadata = get_pexels_images(query="mountains")

        # Get specific photo by ID
        urls, metadata = get_pexels_images(photo_id="2014422")

        # Get photos by specific photographer
        urls, metadata = get_pexels_images(query="nature", photographer="John Doe")
    """
    headers = {
        'Authorization': 't0NmJLY2HIIgOP5I2tzWwCyt1rYyif5Vke7VYNEZqoV7aPCVecbbK8tW'
    }

    if photo_id:
        # If photo_id is provided, use photos endpoint directly
        response = requests.get(
            f'https://api.pexels.com/v1/photos/{photo_id}',
            headers=headers
        )
        photo = response.json()
        if photographer is None or photo['photographer'] == photographer:
            return [photo['src']['original']], [photo]
        return [], []

    # Build query parameters for search
    params = {
        'query': query,
        'per_page': limit
    }

    response = requests.get(
        'https://api.pexels.com/v1/search?query=nature',
        headers=headers,
        params=params
    )

    photos = response.json()['photos']
    result = []

    for photo in photos:
        if photographer is None or photo['photographer'] == photographer:
            result.append(photo)
            if len(result) >= limit:
                break

    original_photos = [photo['src']['original'] for photo in result]
    return original_photos, result

def get_unsplash_images(query=None, photo_id=None, photographer=None, limit=10):
    """Get images from Unsplash API.
    
    Args:
        query (str, optional): Search query for photos. Defaults to None.
        photo_id (str, optional): Specific photo ID to retrieve. Defaults to None.
        photographer (str, optional): Filter by photographer name. Defaults to None.
        limit (int, optional): Maximum number of photos to return. Defaults to 10.

    Returns:
        tuple: Lists of photo URLs and metadata
    """
    key = 'qrb9ob5MxEzMXiA-C11TVOrYGiob2NwEzpHiAXGwmRU'

    if photo_id:
        response = requests.get(
            f'https://api.unsplash.com/photos/{photo_id}',
            params={'client_id': key}
        )
        photo = response.json()
        if photographer is None or photo['user']['name'] == photographer:
            return [photo['urls']['raw']], [photo]
        return [], []

    params = {
        'client_id': key,
        'per_page': limit
    }
    if query:
        params['query'] = query
        endpoint = 'search/photos'
    else:
        endpoint = 'photos'

    response = requests.get(
        f'https://api.unsplash.com/{endpoint}',
        params=params
    )
    
    if query:
        photos = response.json()['results']
    else:
        photos = response.json()
        
    result = []
    for photo in photos:
        if photographer is None or photo['user']['name'] == photographer:
            result.append(photo)
            if len(result) >= limit:
                break

    photo_urls = [photo['urls']['raw'] for photo in result]
    return photo_urls, result

def process_images(get_images_func, **context):
    """Get images from API and return URLs and metadata"""
    urls, metadata = get_images_func(limit=5)
    return {'urls': urls, 'metadata': metadata}

with DAG(
    dag_id="get_API_raw_data",
    start_date=datetime.datetime.now(),  # Start today
    end_date=datetime.datetime.now() + datetime.timedelta(days=3),  # End in 3 days
    schedule_interval="0 0,12 * * *",  # Twice a day: midnight and noon
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
        provide_context=True,
        op_kwargs={'task_ids': 'pexels_start'}
    )
    # Task to push Unsplash data to MongoDB
    unsplash_to_mongo = PythonOperator(
        task_id="unsplash_to_mongo",
        python_callable=push_to_mongodb,
        provide_context=True,
        op_kwargs={'task_ids': 'unsplash_start'}
    )

    # Set task dependencies
    pexels_start >> pexels_to_mongo
    unsplash_start >> unsplash_to_mongo
