import cloudinary
import cloudinary.uploader
from typing import List, Dict
from utils.logger import logger
from config.settings import CLOUDINARY


def configure_cloudinary():
    """Configure Cloudinary if credentials are available"""
    try:
        if all([CLOUDINARY.CLOUD_NAME, CLOUDINARY.API_KEY, CLOUDINARY.API_SECRET]):
            cloudinary.config(
                cloud_name=CLOUDINARY.CLOUD_NAME,
                api_key=CLOUDINARY.API_KEY,
                api_secret=CLOUDINARY.API_SECRET,
                secure=CLOUDINARY.SECURE
            )
            return True
    except Exception as e:
        logger.warning(f"Cloudinary configuration failed: {e}")
    return False


def upload_images_to_cloudinary(images: List[Dict], folder: str = "products") -> List[Dict]:
    """
    Upload images to Cloudinary
    
    Args:
        images: List of image dictionaries with 'url' key
        folder: Cloudinary folder path
        
    Returns:
        List of image dictionaries with Cloudinary URLs
    """
    if not configure_cloudinary():
        return images
    
    uploaded_images = []
    
    for i, image in enumerate(images):
        image_url = image.get('url', '')
        
        if not image_url or not image_url.startswith(('http://', 'https://')):
            # Skip local paths or empty URLs
            uploaded_images.append(image)
            continue
        
        try:
            # Upload to Cloudinary
            result = cloudinary.uploader.upload(
                image_url,
                folder=folder,
                public_id=f"image_{i}",
                overwrite=False,
                resource_type="auto"
            )
            
            # Get secure URL
            cloudinary_url = result.get('secure_url', '')
            
            if cloudinary_url:
                uploaded_images.append({
                    'url': cloudinary_url,
                    'alt': image.get('alt', ''),
                    'position': image.get('position', i),
                    'cloudinary_id': result.get('public_id')
                })
                logger.debug(f"Uploaded image to Cloudinary: {cloudinary_url}")
            else:
                uploaded_images.append(image)
                
        except Exception as e:
            logger.warning(f"Failed to upload image to Cloudinary: {e}")
            uploaded_images.append(image)
    
    return uploaded_images


# def extract_image_urls_from_magento(media_gallery_entries: List[Dict], 
#                                    base_url: str = None) -> List[Dict]:
#     """
#     Extract and format image URLs from Magento media gallery
    
#     Args:
#         media_gallery_entries: Magento media gallery entries
#         base_url: Optional base URL for relative paths
        
#     Returns:
#         List of formatted image dictionaries
#     """
#     images = []
    
#     for entry in media_gallery_entries:
#         file_path = entry.get('file', '')
#         if not file_path:
#             continue
        
#         # Format URL
#         if file_path.startswith(('http://', 'https://')):
#             image_url = file_path
#         elif base_url:
#             image_url = f"{base_url.rstrip('/')}/media/catalog/product{file_path}"
#         else:
#             image_url = f"/media/catalog/product{file_path}"
        
#         images.append({
#             'url': image_url,
#             'alt': entry.get('label', ''),
#             'position': entry.get('position', 0),
#             'types': entry.get('types', []),
#             'disabled': entry.get('disabled', False)
#         })
    
#     # Sort by position
#     images.sort(key=lambda x: x.get('position', 0))
    
#     return images