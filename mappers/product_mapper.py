from typing import Any, Dict, List, Optional, Set
from mappers.base_mapper import BaseMapper
from core.transformer import Transformer
from core.validator import Validator
from utils.logger import logger
from mappers.utils.text_utils import html_to_text
import re


class ProductMapper(BaseMapper):
    """Mapper for Magento to Medusa product conversion"""
    
    def __init__(self, source_connector=None, target_connector=None):
        super().__init__('product_mapping.yaml', source_connector, target_connector)
        self.transformer = Transformer()
        self.validator = Validator()
        self.value_maps: Dict[str, Dict[str, str]] = {}
        
    def map(self, source_data: Dict[str, Any], context: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Map Magento product to Medusa format
        
        Args:
            source_data: Magento product data
            context: Additional context (e.g., category mapping, child products)
            
        Returns:
            Dict: Medusa formatted product
        """
        if context is None:
            context = {}
            
        # Get product type
        product_type = source_data.get('type_id', 'simple')
        
        # Initialize result
        result = {
            "type": "product",
            "status": "draft",
            "metadata": {},
            "images": [],
            "tags": [],
            "collection": None
        }
        
        # Process basic fields
        self._map_basic_fields(result, source_data, context)
        
        # Process custom attributes
        self._map_custom_attributes(result, source_data)
        
        # Process images
        self._map_images(result, source_data)
        
        # Process categories
        self._map_categories(result, source_data, context)
        
        # Process product type specific data
        self._process_product_type(result, source_data, context)
        
        # Apply custom transformations
        self._apply_custom_transformations(result, source_data, context)
        
        # Add metadata
        result = self._add_metadata(result, source_data, context)
        
        # Validate result
        self._validate_result(result)
        
        return result
    
    def _map_basic_fields(self, result: Dict, source_data: Dict, context: Dict):
        """Map basic product fields"""
        fields_config = self.mapping_config.get('fields', {})
        
        for source_field, field_config in fields_config.items():
            if source_field not in source_data:
                continue
                
            target_field = field_config.get('target')
            if not target_field:
                continue
                
            source_value = source_data[source_field]
            transform = field_config.get('transform')
            
            # Apply transformation
            if transform:
                transformed_value = self._apply_field_transformation(
                    source_value, transform, source_field, source_data, context
                )
            else:
                transformed_value = source_value
                
            # Set value (handle nested paths and arrays)
            self._set_field_value(result, target_field, transformed_value)
    
    def _apply_field_transformation(self, value: Any, transform: str, 
                                   field_name: str, source_data: Dict, 
                                   context: Dict) -> Any:
        """Apply field-specific transformation"""
        try:
            if transform == 'clean_sku':
                return self.transformer.clean_sku(value)
            elif transform == 'strip':
                return self.transformer.strip(value)
            elif transform == 'html_to_text':
                return html_to_text(value)
            elif transform == 'slugify':
                return self.transformer.slugify(value)
            elif transform == 'normalize_price':
                return self.transformer.normalize_price(value)
            elif transform == 'map_status':
                return "published" if value == 1 else "draft"
            elif transform == 'map_inventory':
                return bool(value)  # is_in_stock to manage_inventory
            elif transform == 'truncate':
                # Extract parameters from transform like "truncate(100)"
                match = re.match(r'truncate\((\d+)\)', transform)
                if match:
                    max_len = int(match.group(1))
                    return self.transformer.truncate(value, max_len)
                return value
            else:
                # Check if transformer has this method
                if hasattr(self.transformer, transform):
                    return getattr(self.transformer, transform)(value)
                return value
        except Exception as e:
            logger.warning(f"Transformation '{transform}' failed for field '{field_name}': {e}")
            return value
    
    def _set_field_value(self, obj: Dict, path: str, value: Any):
        """Set value for field path, supporting arrays and nested objects"""
        if '[' in path:
            # Handle array paths like "variants[0].prices[0].amount"
            self._set_array_field_value(obj, path, value)
        elif '.' in path:
            # Handle nested paths like "metadata.special_price"
            parts = path.split('.')
            current = obj
            for part in parts[:-1]:
                if part not in current:
                    current[part] = {}
                elif not isinstance(current[part], dict):
                    current[part] = {}
                current = current[part]
            current[parts[-1]] = value
        else:
            obj[path] = value
    
    def _set_array_field_value(self, obj: Dict, path: str, value: Any):
        """Set value for array field path"""
        # Parse path like "variants[0].prices[0].amount"
        parts = path.split('.')
        current = obj
        
        for i, part in enumerate(parts):
            if '[' in part:
                # Extract array name and index
                array_name, index_str = part[:-1].split('[')
                index = int(index_str)
                
                # Ensure array exists
                if array_name not in current:
                    current[array_name] = []
                
                # Extend array if needed
                while len(current[array_name]) <= index:
                    current[array_name].append({})
                
                # Move to array element
                if i == len(parts) - 1:
                    # Last part - set value
                    current[array_name][index] = value
                else:
                    # Intermediate part - move deeper
                    current = current[array_name][index]
            else:
                if i == len(parts) - 1:
                    current[part] = value
                else:
                    if part not in current:
                        current[part] = {}
                    current = current[part]
    
    def _map_custom_attributes(self, result: Dict, source_data: Dict):
        """Map custom attributes to metadata"""
        custom_attrs = source_data.get('custom_attributes', [])
        
        for attr in custom_attrs:
            attr_code = attr.get('attribute_code')
            attr_value = attr.get('value')
            
            if attr_code and attr_value is not None:
                # Map known attributes to specific fields
                if attr_code in ['color', 'size', 'material', 'brand']:
                    # These become product options or metadata
                    if 'metadata' not in result:
                        result['metadata'] = {}
                    result['metadata'][attr_code] = attr_value
                else:
                    # Store in metadata with prefix
                    metadata_key = f"magento_{attr_code}"
                    if 'metadata' not in result:
                        result['metadata'] = {}
                    result['metadata'][metadata_key] = attr_value
    
    def _map_images(self, result: Dict, source_data: Dict):
        """Map product images"""
        gallery_entries = source_data.get('media_gallery_entries', [])
        images = []
        
        for entry in gallery_entries:
            image_data = {
                'url': entry.get('file', ''),
                'alt': entry.get('label', ''),
                'position': entry.get('position', 0)
            }
            
            # Only add if URL exists
            if image_data['url']:
                # Prepend media path if needed
                if not image_data['url'].startswith(('http://', 'https://', '/')):
                    image_data['url'] = f"/media/catalog/product{image_data['url']}"
                images.append(image_data)
        
        # Sort by position
        images.sort(key=lambda x: x.get('position', 0))
        result['images'] = images
        
        # Set thumbnail if images exist
        if images:
            result['thumbnail'] = images[0]['url']
    
    def _map_categories(self, result: Dict, source_data: Dict, context: Dict):
        """Map product categories"""
        category_ids = []
        
        # Get category IDs from various sources
        if 'category_ids' in source_data:
            category_ids.extend(source_data['category_ids'])
        elif 'extension_attributes' in source_data:
            cat_attrs = source_data['extension_attributes']
            if 'category_links' in cat_attrs:
                for link in cat_attrs['category_links']:
                    category_ids.append(link.get('category_id'))
        
        # Map to Medusa category IDs
        id_mapping = context.get('id_mapping', {})
        medusa_categories = []
        
        for cat_id in category_ids:
            magento_id = str(cat_id)
            if magento_id in id_mapping:
                medusa_categories.append({
                    'id': id_mapping[magento_id]
                })
        
        if medusa_categories:
            result['categories'] = medusa_categories
    
    def _process_product_type(self, result: Dict, source_data: Dict, context: Dict):
        """Process product type specific data"""
        product_type = source_data.get('type_id', 'simple')
        
        if product_type == 'configurable':
            self._process_configurable_product(result, source_data, context)
        elif product_type == 'bundle':
            self._process_bundle_product(result, source_data, context)
        elif product_type == 'grouped':
            self._process_grouped_product(result, source_data, context)
        else:
            # Simple product
            self._process_simple_product(result, source_data)
    
    def _process_simple_product(self, result: Dict, source_data: Dict):
        """Process simple product"""
        # Ensure variants array exists
        if 'variants' not in result:
            result['variants'] = []
        
        # Create single variant
        variant = {
            'title': result.get('title', 'Default Variant'),
            'sku': source_data.get('sku', ''),
            'manage_inventory': True,
            'allow_backorder': False,
            'inventory_quantity': int(source_data.get('quantity', 0)),
            'prices': self._build_prices(source_data),
            'options': {},
            'metadata': {}
        }
        
        # Add weight if available
        weight = source_data.get('weight')
        if weight:
            variant['weight'] = float(weight)
        
        result['variants'].append(variant)
    
    def _process_configurable_product(self, result: Dict, source_data: Dict, context: Dict):
        """Process configurable product with variants"""
        # Build value maps for attribute options
        self._build_value_maps(source_data)
        
        # Build parent options
        parent_options = self._build_parent_options(source_data)
        if parent_options:
            result['options'] = parent_options
        
        # Build variants from child products
        child_products = context.get('child_products', {})
        child_ids = source_data.get('extension_attributes', {}) \
                              .get('configurable_product_links', [])
        
        variants = []
        for child_id in child_ids:
            child = child_products.get(child_id)
            if child:
                variant = self._build_variant(child, parent_options)
                if variant:
                    variants.append(variant)
        
        if variants:
            result['variants'] = variants
        else:
            # Fallback to simple product if no variants
            self._process_simple_product(result, source_data)
    
    def _build_value_maps(self, product: Dict):
        """Build maps for attribute value to label"""
        config_options = product.get('extension_attributes', {}) \
                                .get('configurable_product_options', [])
        
        for option in config_options:
            attr_code = option.get('attribute_code')
            if not attr_code:
                continue
                
            # Fetch attribute options from Magento
            try:
                if self.source_connector:
                    options = self.source_connector.get_attribute_options(attr_code)
                    value_map = {}
                    for opt in options:
                        value = str(opt.get('value'))
                        label = opt.get('label', '').strip() or f"Option {value}"
                        value_map[value] = label
                    self.value_maps[attr_code] = value_map
            except Exception as e:
                logger.warning(f"Failed to fetch options for {attr_code}: {e}")
    
    def _build_parent_options(self, product: Dict) -> List[Dict]:
        """Build parent product options"""
        config_options = product.get('extension_attributes', {}) \
                                .get('configurable_product_options', [])
        
        options = []
        for config_opt in config_options:
            attr_code = config_opt.get('attribute_code')
            label = config_opt.get('label', attr_code)
            
            option_values = []
            for value_data in config_opt.get('values', []):
                value_index = str(value_data.get('value_index'))
                # Use mapped label if available
                display_value = self.value_maps.get(attr_code, {}).get(value_index, value_index)
                option_values.append(display_value)
            
            if option_values:
                options.append({
                    'title': label,
                    'values': option_values
                })
        
        return options
    
    def _build_variant(self, child: Dict, parent_options: List[Dict]) -> Optional[Dict]:
        """Build variant from child product"""
        if not child:
            return None
        
        # Map variant options
        variant_options = {}
        custom_attrs = child.get('custom_attributes', [])
        
        for attr in custom_attrs:
            attr_code = attr.get('attribute_code')
            attr_value = str(attr.get('value'))
            
            # Find matching parent option
            for parent_opt in parent_options:
                if parent_opt['title'].lower() == attr_code.lower():
                    # Get display value from value maps
                    display_value = self.value_maps.get(attr_code, {}).get(attr_value, attr_value)
                    variant_options[parent_opt['title']] = display_value
                    break
        
        # Build variant title
        base_title = child.get('name', 'Variant')
        if variant_options:
            option_str = ' '.join(variant_options.values())
            variant_title = f"{base_title} - {option_str}"
        else:
            variant_title = base_title
        
        variant = {
            'title': variant_title,
            'sku': child.get('sku', ''),
            'manage_inventory': True,
            'allow_backorder': False,
            'inventory_quantity': int(child.get('quantity', 0)),
            'prices': self._build_prices(child),
            'options': variant_options,
            'metadata': {}
        }
        
        # Add weight
        weight = child.get('weight')
        if weight:
            variant['weight'] = float(weight)
        
        return variant
    
    def _build_prices(self, product: Dict) -> List[Dict]:
        """Build price array for product/variant"""
        price = product.get('price', 0)
        try:
            amount = self.transformer.normalize_price(price)
        except Exception:
            amount = 0
        
        return [{
            'currency_code': 'usd',
            'amount': amount
        }]
    
    def _process_bundle_product(self, result: Dict, source_data: Dict, context: Dict):
        """Process bundle product"""
        # For now, treat bundle as simple product
        # In production, you'd want to map bundle items
        self._process_simple_product(result, source_data)
        result['metadata']['is_bundle'] = True
    
    def _process_grouped_product(self, result: Dict, source_data: Dict, context: Dict):
        """Process grouped product"""
        # For now, treat grouped as simple product
        self._process_simple_product(result, source_data)
        result['metadata']['is_grouped'] = True
    
    def _apply_custom_transformations(self, result: Dict, source_data: Dict, context: Dict):
        """Apply custom transformations from config"""
        transformations = self.mapping_config.get('transformations', {})
        
        for target_field, rule in transformations.items():
            try:
                if rule == 'first_image_url':
                    if result.get('images'):
                        result['thumbnail'] = result['images'][0]['url']
                
                elif rule.startswith('if_empty('):
                    func_call = rule[9:-1]  # Remove if_empty()
                    if target_field not in result or not result[target_field]:
                        if '(' in func_call:
                            func_name = func_call.split('(')[0]
                            arg = func_call.split('(')[1][:-1]
                            value = source_data.get(arg, '')
                            result[target_field] = getattr(self.transformer, func_name)(value)
                
                elif rule == 'build_variants(type_id, configurable_children)':
                    # Already handled in _process_product_type
                    pass
                    
            except Exception as e:
                logger.warning(f"Custom transformation failed for {target_field}: {e}")
    
    def _add_metadata(self, result: Dict, source_data: Dict, context: Dict) -> Dict:
        """Add additional metadata"""
        if 'metadata' not in result:
            result['metadata'] = {}
        
        # Add source system info
        result['metadata']['source_system'] = self.source_system
        result['metadata']['source_id'] = source_data.get('id')
        result['metadata']['source_type'] = source_data.get('type_id')
        
        # Add timestamps
        if 'created_at' in source_data:
            result['metadata']['source_created_at'] = source_data['created_at']
        if 'updated_at' in source_data:
            result['metadata']['source_updated_at'] = source_data['updated_at']
        
        # Add sync context
        result['metadata']['sync_timestamp'] = context.get('sync_timestamp')
        result['metadata']['sync_batch'] = context.get('batch_id')
        
        return result
    
    def _validate_result(self, result: Dict):
        """Validate mapped result"""
        # Check required fields
        required_fields = ['title', 'sku']
        for field in required_fields:
            if field not in result or not result[field]:
                logger.warning(f"Product missing required field: {field}")
        
        # Check variant structure
        if 'variants' in result:
            for variant in result['variants']:
                if 'sku' not in variant or not variant['sku']:
                    logger.warning("Variant missing SKU")
                if 'prices' not in variant or not variant['prices']:
                    logger.warning("Variant missing prices")
    
    def map_variant(self, variant_data: Dict, parent_product: Dict = None) -> Dict:
        """Map individual variant data"""
        result = {
            'title': variant_data.get('name', 'Variant'),
            'sku': self.transformer.clean_sku(variant_data.get('sku', '')),
            'manage_inventory': True,
            'allow_backorder': False,
            'prices': self._build_prices(variant_data),
            'options': {},
            'metadata': {}
        }
        
        # Add inventory quantity
        quantity = variant_data.get('quantity', 0)
        if quantity is not None:
            result['inventory_quantity'] = int(quantity)
        
        # Add weight
        weight = variant_data.get('weight')
        if weight:
            result['weight'] = float(weight)
        
        # Map variant options if parent provided
        if parent_product and 'options' in parent_product:
            # This would map variant-specific options
            pass
        
        return result