from sys import prefix
import cloudinary
import cloudinary.uploader
from typing import List, Dict
from utils.logger import logger
from config.settings import CLOUDINARY
from config.settings import MAGENTO
from pathlib import Path


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

def resolve_local_image_path(image_url: str) -> Path | None:
    """
    Convert Magento image path (/m/b/file.jpg)
    to local filesystem path
    """
    if not image_url:
        return None

    # remove leading slash
    relative_path = image_url.lstrip("/")

    local_path = MAGENTO.MAGENTO_MEDIA_ROOT / relative_path

    if local_path.exists() and local_path.is_file():
        return local_path

    return None

def upload_images_to_cloudinary(images: List[Dict], folder="products", prefix="product") -> List[Dict]:
    if not configure_cloudinary():
        return images

    uploaded_images = []

    for i, image in enumerate(images):
        url = image.get("url", "")
        image_url = f"/{prefix}{url}"
    
        local_path = resolve_local_image_path(image_url)

        try:
            if local_path:
                result = cloudinary.uploader.upload(
                    str(local_path),
                    folder=folder,
                    public_id=f"product_{i}",
                    overwrite=False,
                    resource_type="image"
                )
            elif image_url.startswith(("http://", "https://")):
                # Upload URL
                result = cloudinary.uploader.upload(
                    image_url,
                    folder=folder,
                    public_id=f"product_{i}",
                    overwrite=False,
                    resource_type="image"
                )
            else:
                # Không xử lý được
                uploaded_images.append(image)
                continue

            cloudinary_url = result.get("secure_url")

            uploaded_images.append({
                "url": cloudinary_url,
                "alt": image.get("alt", ""),
                "position": image.get("position", i),
                "cloudinary_id": result.get("public_id"),
            })

            logger.info(f"Uploaded image to Cloudinary: {cloudinary_url}")

        except Exception as e:
            logger.warning(f"Failed to upload image {image_url}: {e}")
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