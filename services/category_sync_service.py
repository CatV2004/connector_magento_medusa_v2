from typing import Dict, Optional
from utils.logger import logger
from mappers.category_mapper import CategoryMapper
from core.dlq_handler import DLQHandler
from connectors.magento.magento_connector import MagentoConnector
from connectors.medusa.medusa_connector import MedusaConnector
from datetime import datetime
from core.mapping.mapping_factory import MappingFactory
from pathlib import Path

class CategorySyncService:
    """Service for syncing categories from Magento to Medusa"""
    
    def __init__(self, magento: MagentoConnector, medusa: MedusaConnector):
        self.magento = magento
        self.medusa = medusa
        logger.info("Initializing CategorySyncService")
        print("Initializing CategorySyncService")

        mapping = MappingFactory(
            Path("config/mapping")
        ).get("category")
        logger.debug(f"Category mapping config: {mapping}") 

        self.mapper = CategoryMapper(
            mapping_config=mapping,
            source_connector=magento,
            target_connector=medusa
        )
        self.dlq = DLQHandler('categories')
        self.sync_stats = {
            'total_processed': 0,
            'successful': 0,
            'failed': 0,
            'skipped': 0
        }
        
    def sync_all(self, root_category_id: int = 2) -> Dict:
        logger.info("Starting category sync from Magento to Medusa...")
        
        try:
            # Load existing Medusa categories
            self._load_existing_medusa_categories()
            
            # Start sync from root
            self._sync_category_tree(root_category_id)
            
            # Process DLQ items if any
            self._process_dlq_items()
            
            logger.info(f"Category sync completed. Stats: {self.sync_stats}")
            return {
                'stats': self.sync_stats,
                'mapping': self.mapper.get_id_mapping(),
                'dlq_count': self.dlq.get_count()
            }
            
        except Exception as e:
            logger.error(f"Category sync failed: {e}")
            raise
            
    def _load_existing_medusa_categories(self):
        """Load existing Medusa categories for deduplication"""
        try:
            existing_categories = self.medusa.get_categories(limit=1000)
            self.existing_by_name = {cat['name']: cat for cat in existing_categories}
            self.existing_by_handle = {cat['handle']: cat for cat in existing_categories}
            logger.info(f"Loaded {len(existing_categories)} existing Medusa categories")
        except Exception as e:
            logger.warning(f"Failed to load existing Medusa categories: {e}")
            self.existing_by_name = {}
            self.existing_by_handle = {}
            
    def _sync_category_tree(self, parent_id: int, level: int = 0):
        try:
            # Get child categories from Magento
            logger.info(f"Fetching categories under parent ID {parent_id}")
            magento_categories = self.magento.get_categories_by_parent(parent_id)
            
            if not magento_categories:
                logger.debug(f"No child categories found for parent {parent_id}")
                return
                
            logger.info(f"Processing {len(magento_categories)} categories "
                       f"under parent {parent_id} (level {level})")
            
            # Sort by position
            magento_categories.sort(key=lambda x: x.get('position', 0))
            
            # Sync each category
            for mag_category in magento_categories:
                self._sync_single_category(mag_category, level)
                
            # Recursively sync children
            for mag_category in magento_categories:
                if mag_category['id'] not in (1, 2):  # Skip root categories
                    self._sync_category_tree(mag_category['id'], level + 1)
                    
        except Exception as e:
            logger.error(f"Failed to sync category tree for parent {parent_id}: {e}")
            raise
            
    def _sync_single_category(self, mag_category: Dict, level: int = 0):
        logger.info(f"Syncing category {mag_category}")
        magento_id = str(mag_category['id'])
        category_name = mag_category.get('name', 'Unnamed Category')
        logger.info(f"Syncing category: {category_name} (ID: {magento_id})")
        
        # Skip root categories
        if mag_category['id'] in (1, 2):
            logger.debug(f"Skipping root category: {category_name} (ID: {magento_id})")
            self.sync_stats['skipped'] += 1
            return
            
        # Update stats
        self.sync_stats['total_processed'] += 1
        
        try:
            # Validate category data
            validation_errors = self.mapper.validate(mag_category)
            if validation_errors['missing_required']:
                error_msg = f"Missing required fields: {validation_errors['missing_required']}"
                logger.warning(f"Category {category_name} validation issues: {error_msg}")
                
                # Add to DLQ for manual review
                self.dlq.add_item({
                    'source_data': mag_category,
                    'errors': validation_errors,
                    'operation': 'create',
                    'timestamp': datetime.now().isoformat()
                })
                self.sync_stats['failed'] += 1
                return
            
            # Check if already exists in Medusa
            existing_category = self._find_existing_category(mag_category)
            if existing_category:
                logger.info(f"Category exists, updating: {category_name}")
                self._update_category(existing_category['id'], mag_category)
                medusa_id = existing_category['id']
            else:
                logger.info(f"Creating new category: {category_name}")
                medusa_id = self._create_category(mag_category)
            
            # Update ID mapping for parent-child relationships
            self.mapper.update_id_mapping(magento_id, medusa_id)
            self.sync_stats['successful'] += 1
            
            # Log progress
            indent = "  " * level
            logger.info(f"{indent}✓ {category_name} → {medusa_id}")
            
        except Exception as e:
            logger.error(f"Failed to sync category {category_name} (ID: {magento_id}): {e}")
            self.sync_stats['failed'] += 1
            
            # Add to DLQ
            self.dlq.add_item({
                'source_data': mag_category,
                'error': str(e),
                'operation': 'sync',
                'timestamp': datetime.now().isoformat()
            })
            
    # def _find_existing_category(self, mag_category: Dict) -> Optional[Dict]:
    #     """Find existing category in Medusa by name or handle"""
    #     category_name = mag_category.get('name')
        
    #     # Try to find by name
    #     if category_name and category_name in self.existing_by_name:
    #         return self.existing_by_name[category_name]
            
    #     # Try to find by handle
    #     url_key = mag_category.get('url_key')
    #     if url_key:
    #         handle = self.mapper.transformer.slugify(url_key)
    #         if handle in self.existing_by_handle:
    #             return self.existing_by_handle[handle]
                
    #     return None
    def _find_existing_category(self, mag_category: Dict) -> Optional[Dict]:
        category_name = mag_category.get('name')
        magento_id = str(mag_category.get('id'))
        
        if magento_id in self.mapper.get_id_mapping():
            medusa_id = self.mapper.get_id_mapping()[magento_id]
            for cat in self.existing_by_name.values():
                if cat.get('id') == medusa_id:
                    return cat
        
        parent_id = mag_category.get('parent_id')
        if category_name and parent_id:
            # Map Magento parent_id to Medusa parent_id
            medusa_parent_id = self.mapper.get_id_mapping().get(str(parent_id))
            
            for cat in self.existing_by_name.values():
                # Check name match
                if cat.get('name') == category_name:
                    # Check parent match
                    cat_parent_id = cat.get('parent_category_id')
                    if (medusa_parent_id and cat_parent_id and 
                        str(cat_parent_id) == str(medusa_parent_id)):
                        return cat
                    elif not medusa_parent_id and not cat_parent_id:
                        return cat
        
        if category_name and category_name in self.existing_by_name:
            return self.existing_by_name[category_name]
            
        url_key = mag_category.get('url_key')
        if url_key:
            handle = self.mapper.transformer.slugify(url_key)
            if handle in self.existing_by_handle:
                return self.existing_by_handle[handle]
                
        return None
        
    def _create_category(self, mag_category: Dict) -> str:
        """Create category in Medusa"""
        # Prepare context for mapper
        context = {
            'id_mapping': self.mapper.get_id_mapping(),
            'operation': 'create'
        }
        
        # Map Magento data to Medusa format
        medusa_data = self.mapper.map(mag_category, context)
        logger.info(f"Mapped Medusa data: {medusa_data}")
        
        # Create in Medusa
        response = self.medusa.create_category(medusa_data)
        
        if 'product_category' in response:
            new_category = response['product_category']
            category_id = new_category['id']
            category_name = new_category['name']
            category_handle = new_category.get('handle', '')
            
            # Update local cache
            self.existing_by_name[category_name] = new_category
            if category_handle:
                self.existing_by_handle[category_handle] = new_category 
            
            if not hasattr(self, 'existing_by_id'):
                self.existing_by_id = {}
            self.existing_by_id[category_id] = new_category
            
            return category_id
        else:
            raise Exception(f"Unexpected response format: {response}")
            
    def _update_category(self, category_id: str, mag_category: Dict):
        """Update existing category in Medusa"""
        # Prepare context for mapper
        context = {
            'id_mapping': self.mapper.get_id_mapping(),
            'operation': 'update'
        }
        
        # Map Magento data to Medusa format
        medusa_data = self.mapper.map(mag_category, context)
        
        # Update in Medusa
        self.medusa.update_category(category_id, medusa_data)
        
    def _process_dlq_items(self):
        """Process items in DLQ (Dead Letter Queue)"""
        dlq_count = self.dlq.get_count()
        if dlq_count > 0:
            logger.warning(f"Found {dlq_count} items in DLQ. Manual review required.")
            # In production, you might want to send notifications or retry logic here
            
    def get_stats(self) -> Dict:
        """Get current sync statistics"""
        return self.sync_stats.copy()
        
    def get_id_mapping(self) -> Dict[str, str]:
        """Get ID mapping dictionary"""
        return self.mapper.get_id_mapping()