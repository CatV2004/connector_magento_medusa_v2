from typing import Dict, List, Optional, Set
from datetime import datetime
import time
from utils.logger import logger
from mappers.product_mapper import ProductMapper
from core.dlq_handler import DLQHandler
from connectors.magento.magento_connector import MagentoConnector
from connectors.medusa.medusa_connector import MedusaConnector
from mappers.utils.image_utils import upload_images_to_cloudinary
from core.mapping.mapping_factory import MappingFactory
from pathlib import Path

class ProductSyncService:
    """Service for syncing products from Magento to Medusa"""
    
    def __init__(self, magento: MagentoConnector, medusa: MedusaConnector, 
                 category_mapping: Optional[Dict] = None):
        self.magento = magento
        self.medusa = medusa
        mapping = MappingFactory(
            Path("config/mapping")
        ).get("product")

        self.mapper = ProductMapper(
            mapping_config=mapping,
            source_connector=magento,
            target_connector=medusa
        )
        self.dlq = DLQHandler('products')
        self.category_mapping = category_mapping or {}
        
        self.sync_stats = {
            'total_processed': 0,
            'successful': 0,
            'failed': 0,
            'skipped': 0,
            'simple_products': 0,
            'configurable_products': 0,
            'variants_created': 0
        }
        
        # Cache for existing products
        self.existing_products: Dict[str, str] = {}  # SKU -> Medusa ID
        self.processed_skus: Set[str] = set()
        
    def sync_all(self, batch_size: int = 50, max_pages: Optional[int] = None) -> Dict:
        logger.info("Starting product sync from Magento to Medusa...")
        logger.info(f"Batch size: {batch_size}, Category mapping: {len(self.category_mapping)} items")
        
        try:
            # Load existing products from Medusa
            self._load_existing_products()
            
            # Sync products in batches
            page = 1
            has_more = True
            
            while has_more and (max_pages is None or page <= max_pages):
                logger.info(f"Processing page {page}...")
                
                # Fetch products from Magento
                magento_products = self.magento.get_products(
                    page=page, 
                    page_size=batch_size
                )
                
                if not magento_products:
                    logger.info("No more products to sync")
                    has_more = False
                    break
                
                # Process batch
                self._process_batch(magento_products, page)
                
                # Increment page
                page += 1
                
                # Small delay to avoid rate limiting
                time.sleep(0.5)
            
            # Process DLQ items
            self._process_dlq_items()
            
            # Log summary
            self._log_sync_summary()
            
            return {
                'stats': self.sync_stats,
                'dlq_count': self.dlq.get_count()
            }
            
        except Exception as e:
            logger.error(f"Product sync failed: {e}")
            raise
    
    def _load_existing_products(self):
        """Load existing products from Medusa for deduplication"""
        try:
            logger.info("Loading existing products from Medusa...")
            
            page = 1
            page_size = 100
            total_loaded = 0
            
            while True:
                products = self.medusa.get_products(page=page, page_size=page_size)
                
                if not products:
                    break
                
                for product in products:
                    sku = product.get('sku')
                    if sku:
                        self.existing_products[sku] = product['id']
                        
                        # Also cache variants
                        for variant in product.get('variants', []):
                            variant_sku = variant.get('sku')
                            if variant_sku:
                                self.existing_products[variant_sku] = product['id']
                
                total_loaded += len(products)
                logger.info(f"Loaded {total_loaded} existing products...")
                
                if len(products) < page_size:
                    break
                    
                page += 1
                
            logger.info(f"Loaded {len(self.existing_products)} existing products/variants from Medusa")
            
        except Exception as e:
            logger.warning(f"Failed to load existing products: {e}")
            self.existing_products = {}
    
    def _process_batch(self, magento_products: List[Dict], batch_number: int):
        """Process a batch of products"""
        logger.info(f"Processing batch {batch_number} with {len(magento_products)} products")
        
        # Group configurable products with their children
        product_groups = self._group_products(magento_products)
        
        # Process each product group
        for product_group in product_groups:
            self._process_product_group(product_group, batch_number)
        
        # Flush DLQ batch if needed
        self.dlq._flush_batch()
    
    def _group_products(self, products: List[Dict]) -> List[Dict]:
        """Group configurable products with their child products"""
        # Create map of all products by ID
        product_map = {p['id']: p for p in products}
        
        groups = []
        
        for product in products:
            product_type = product.get('type_id')
            
            if product_type == 'configurable':
                # Get child product IDs
                child_ids = product.get('extension_attributes', {}) \
                                   .get('configurable_product_links', [])
                
                # Find child products in the current batch
                child_products = {}
                for child_id in child_ids:
                    if child_id in product_map:
                        child_products[child_id] = product_map[child_id]
                
                groups.append({
                    'parent': product,
                    'children': child_products,
                    'type': 'configurable'
                })
            elif product_type in ['simple', 'virtual', 'downloadable']:
                # Check if this is a child of a configurable product
                # If parent is not in this batch, treat as standalone
                parent_id = self._find_parent_id(product, product_map)
                if not parent_id:
                    groups.append({
                        'parent': product,
                        'children': {},
                        'type': 'simple'
                    })
        
        return groups
    
    def _find_parent_id(self, product: Dict, product_map: Dict) -> Optional[int]:
        """Find if product is a child of a configurable product"""
        # In Magento, child products don't explicitly reference parent
        # This would need custom logic based on your setup
        return None
    
    def _process_product_group(self, product_group: Dict, batch_number: int):
        """Process a product group (parent + children)"""
        parent = product_group['parent']
        children = product_group['children']
        group_type = product_group['type']
        
        magento_id = parent['id']
        sku = parent.get('sku', '')
        
        # Skip if already processed
        if sku in self.processed_skus:
            self.sync_stats['skipped'] += 1
            return
        
        # Update stats
        self.sync_stats['total_processed'] += 1
        if group_type == 'configurable':
            self.sync_stats['configurable_products'] += 1
            self.sync_stats['variants_created'] += len(children)
        else:
            self.sync_stats['simple_products'] += 1
        
        try:
            # Prepare context for mapper
            context = {
                'id_mapping': self.category_mapping,
                'child_products': children,
                'batch_id': f'batch_{batch_number}',
                'sync_timestamp': datetime.now().isoformat()
            }
            
            # Map product data
            medusa_data = self.mapper.map(parent, context)
            
            # Handle image uploads (if configured)
            medusa_data = self._process_images(medusa_data, parent)
            
            # Check if product already exists
            existing_id = self.existing_products.get(sku)
            
            if existing_id:
                # Update existing product
                logger.info(f"Updating existing product: {sku}")
                self._update_product(existing_id, medusa_data, sku)
            else:
                # Create new product
                logger.info(f"Creating new product: {sku}")
                self._create_product(medusa_data, sku)
            
            # Mark as processed
            self.processed_skus.add(sku)
            self.sync_stats['successful'] += 1
            
            # Log progress
            product_name = parent.get('name', 'Unnamed Product')
            logger.info(f"✓ {product_name} ({sku})")
            
        except Exception as e:
            logger.error(f"Failed to process product {sku}: {e}")
            self.sync_stats['failed'] += 1
            
            # Add to DLQ
            self.dlq.add_item({
                'source_data': parent,
                'error': str(e),
                'operation': 'sync',
                'batch': batch_number,
                'timestamp': datetime.now().isoformat()
            })
    
    def _process_images(self, medusa_data: Dict, magento_data: Dict) -> Dict:
        try:
            from config.settings import CLOUDINARY
            has_cloudinary = all([
                CLOUDINARY.CLOUD_NAME,
                CLOUDINARY.API_KEY,
                CLOUDINARY.API_SECRET
            ])
            
            logger.info(f"Cloudinary configured: {has_cloudinary}")
            logger.info(f"Images to process: {len(medusa_data.get('images', []))}")
            
            if has_cloudinary and medusa_data.get('images'):
                logger.info(f"Uploading {len(medusa_data['images'])} images to Cloudinary...")
                
                uploaded_images = upload_images_to_cloudinary(
                    medusa_data['images'],
                    folder=f"products/{magento_data.get('sku', 'unknown')}"
                )
                
                logger.info(f"Uploaded {len(uploaded_images)} images successfully")
                
                # Kiểm tra xem URLs có thay đổi không
                for i, (original, uploaded) in enumerate(zip(medusa_data['images'], uploaded_images)):
                    if original.get('url') != uploaded.get('url'):
                        logger.info(f"Image {i}: {original.get('url')[:50]}... -> {uploaded.get('url')[:50]}...")
                
                medusa_data['images'] = uploaded_images
                
                if uploaded_images:
                    medusa_data['thumbnail'] = uploaded_images[0]['url']
                    logger.info(f"Set thumbnail to: {uploaded_images[0]['url'][:50]}...")
            else:
                logger.info("Skipping Cloudinary upload (no images or not configured)")
        
        except ImportError as e:
            logger.error(f"Import error: {e}")
            logger.info("Cloudinary not configured, using local image paths")
        except Exception as e:
            logger.error(f"Image processing failed: {e}", exc_info=True)
        
        return medusa_data
    
    def _create_product(self, product_data: Dict, sku: str):
        """Create product in Medusa"""
        try:
            response = self.medusa.create_product(product_data)
            
            if 'product' in response:
                new_product = response['product']
                product_id = new_product['id']
                
                # Update cache
                self.existing_products[sku] = product_id
                
                # Cache variants too
                for variant in new_product.get('variants', []):
                    variant_sku = variant.get('sku')
                    if variant_sku:
                        self.existing_products[variant_sku] = product_id
                
                logger.debug(f"Created product {sku} with ID: {product_id}")
            else:
                raise Exception(f"Unexpected response format: {response}")
                
        except Exception as e:
            logger.error(f"Failed to create product {sku}: {e}")
            raise
    
    def _update_product(self, product_id: str, product_data: Dict, sku: str):
        """Update existing product in Medusa"""
        try:
            # For simplicity, we're creating new products
            # In production, you'd want to implement proper update logic
            logger.warning(f"Update not implemented for {sku}, creating new...")
            self._create_product(product_data, sku)
            
            # Actually, Medusa API might support updates via PUT
            # response = self.medusa.update_product(product_id, product_data)
            # logger.debug(f"Updated product {sku}")
            
        except Exception as e:
            logger.error(f"Failed to update product {sku}: {e}")
            raise
    
    def _process_dlq_items(self):
        """Process items in DLQ"""
        dlq_count = self.dlq.get_count()
        if dlq_count > 0:
            logger.warning(f"Found {dlq_count} failed products in DLQ")
            
            # Optionally retry failed items
            # self._retry_failed_products()
    
    def _retry_failed_products(self):
        """Retry failed products from DLQ"""
        def retry_callback(item):
            """Callback for retrying failed items"""
            source_data = item.get('source_data')
            if source_data:
                # Extract SKU for logging
                sku = source_data.get('sku', 'unknown')
                logger.info(f"Retrying product: {sku}")
                
                # Re-process the product
                # Note: This is simplified - you'd need proper context
                context = {
                    'id_mapping': self.category_mapping,
                    'is_retry': True
                }
                
                medusa_data = self.mapper.map(source_data, context)
                self._create_product(medusa_data, sku)
        
        # Retry with exponential backoff
        retry_successful = self.dlq.retry_failed_items(retry_callback)
        logger.info(f"Retry completed: {retry_successful} products successful")
    
    def _log_sync_summary(self):
        """Log sync summary"""
        logger.info("=" * 60)
        logger.info("PRODUCT SYNC SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Total processed: {self.sync_stats['total_processed']}")
        logger.info(f"Successful: {self.sync_stats['successful']}")
        logger.info(f"Failed: {self.sync_stats['failed']}")
        logger.info(f"Skipped: {self.sync_stats['skipped']}")
        logger.info(f"Simple products: {self.sync_stats['simple_products']}")
        logger.info(f"Configurable products: {self.sync_stats['configurable_products']}")
        logger.info(f"Variants created: {self.sync_stats['variants_created']}")
        logger.info(f"DLQ items: {self.dlq.get_count()}")
        logger.info("=" * 60)
    
    def get_stats(self) -> Dict:
        """Get current sync statistics"""
        return self.sync_stats.copy()