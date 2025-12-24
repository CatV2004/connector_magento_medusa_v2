import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
env_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(env_path)


def get_env(var_name: str, default=None, mandatory=False):
    """Get environment variable with validation"""
    value = os.getenv(var_name, default)
    if mandatory and value is None:
        raise ValueError(f"Environment variable {var_name} is required but not set")
    return value


class MagentoSettings:
    """Magento configuration"""
    BASE_URL = get_env('MAGENTO_BASE_URL', mandatory=True)
    TOKEN = get_env('MAGENTO_TOKEN', mandatory=True)
    ADMIN_USERNAME = get_env('MAGENTO_ADMIN_USERNAME', 'admin')
    ADMIN_PASSWORD = get_env('MAGENTO_ADMIN_PASSWORD', '')
    VERIFY_SSL = get_env('MAGENTO_VERIFY_SSL', 'false').lower() == 'true'
    TIMEOUT = int(get_env('MAGENTO_TIMEOUT', '30'))
    MAGENTO_MEDIA_ROOT = Path(
            "D:/internship/connector_magento_medusa_v2/media/catalog"
        )    
    # API endpoints
    ENDPOINTS = {
        'products': 'products',
        'categories': 'categories',
        'customers': 'customers',
        'orders': 'orders',
        'attributes': 'products/attributes'
    }


class MedusaSettings:
    """Medusa configuration"""
    BASE_URL = get_env('MEDUSA_BASE_URL', mandatory=True)
    API_KEY = get_env('MEDUSA_API_KEY', mandatory=True)
    ADMIN_EMAIL = get_env('MEDUSA_ADMIN_EMAIL', '')
    ADMIN_PASSWORD = get_env('MEDUSA_ADMIN_PASSWORD', '')
    TIMEOUT = int(get_env('MEDUSA_TIMEOUT', '30'))
    
    # API endpoints
    ENDPOINTS = {
        'products': 'products',
        'categories': 'product-categories',
        'customers': 'customers',
        'orders': 'orders'
    }


class CloudinarySettings:
    """Cloudinary configuration for image uploads"""
    CLOUD_NAME = get_env('CLOUDINARY_CLOUD_NAME')
    API_KEY = get_env('CLOUDINARY_API_KEY')
    API_SECRET = get_env('CLOUDINARY_API_SECRET')
    SECURE = get_env('CLOUDINARY_SECURE', 'true').lower() == 'true'
    
    @classmethod
    def is_configured(cls):
        """Check if Cloudinary is configured"""
        return all([cls.CLOUD_NAME, cls.API_KEY, cls.API_SECRET])


class SyncSettings:
    """Sync configuration"""
    BATCH_SIZE = int(get_env('SYNC_BATCH_SIZE', '50'))
    MAX_RETRIES = int(get_env('SYNC_MAX_RETRIES', '3'))
    RETRY_DELAY = int(get_env('SYNC_RETRY_DELAY', '5'))
    DRY_RUN = get_env('SYNC_DRY_RUN', 'false').lower() == 'true'
    
    # Rate limiting
    REQUESTS_PER_MINUTE = int(get_env('REQUESTS_PER_MINUTE', '60'))
    
    # Timeouts
    CONNECTION_TIMEOUT = int(get_env('CONNECTION_TIMEOUT', '30'))
    READ_TIMEOUT = int(get_env('READ_TIMEOUT', '60'))


class LogSettings:
    """Logging configuration"""
    LEVEL = get_env('LOG_LEVEL', 'INFO')
    FILE = get_env('LOG_FILE', 'logs/sync.log')
    FORMAT = get_env('LOG_FORMAT', '%(asctime)s - %(levelname)s - %(message)s')
    MAX_SIZE = int(get_env('LOG_MAX_SIZE', '10485760'))  # 10MB
    BACKUP_COUNT = int(get_env('LOG_BACKUP_COUNT', '5'))


# Export settings classes
MAGENTO = MagentoSettings()
MEDUSA = MedusaSettings()
CLOUDINARY = CloudinarySettings()
SYNC = SyncSettings()
LOG = LogSettings()