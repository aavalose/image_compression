import requests
import numpy as np
from PIL import Image
from io import BytesIO

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
