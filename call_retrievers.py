import requests


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
        'https://api.pexels.com/v1/search',
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

def get_wallhaven_photos(query=None, photo_id=None, limit=10):
    """Retrieve photos from Wallhaven API.

    Args:
        query (str, optional): Search query for photos. Defaults to None.
        photo_id (str, optional): Specific photo ID to retrieve. Defaults to None.
        limit (int, optional): Maximum number of photos to return. Defaults to 10.

    Returns:
        tuple: Lists of photo URLs and metadata
    """
    key = 't7Jx4fJDMjTyIdApWsmAxx5KDmeBskgj'

    if photo_id:
        response = requests.get(
            f'https://wallhaven.cc/api/v1/w/{photo_id}',
            params={'apikey': key}
        )
        photo = response.json()['data']
        return [photo['path']], [photo]

    params = {
        'apikey': key,
        'q': query if query else '',
        'limit': limit
    }

    response = requests.get(
        'https://wallhaven.cc/api/v1/search',
        params=params
    )
    
    photos = response.json()['data']
    
    result = []
    for photo in photos:
        result.append(photo)
        if len(result) >= limit:
            break

    photo_urls = [photo['path'] for photo in result]
    return photo_urls, result