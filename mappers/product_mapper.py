from typing import Any, Dict, List, Optional, Set
from mappers.base_mapper import BaseMapper
from core.transformer import Transformer
from core.validator import Validator
from utils.logger import logger
from mappers.utils.text_utils import html_to_text
import re, json


class ProductMapper(BaseMapper):
    """Mapper for Magento to Medusa product conversion"""
    
    def __init__(
        self, 
        mapping_config: Dict[str, Any],
        source_connector=None, 
        target_connector=None
    ):
        super().__init__(
            mapping_config=mapping_config,
            source_connector=source_connector,
            target_connector=target_connector
        )
        self.transformer = Transformer()
        self.validator = Validator()
        self.value_maps: Dict[str, Dict[str, str]] = {}

    def get_fallback_options(self) -> Dict[str, Any]:
        cfg = self.mapping_config
        return (
            cfg
            .get("defaults", {})
            .get("options", {})
            .get("fallback", {})
        )
        
    def map(self, source_data: Dict[str, Any], context: Optional[Dict] = None) -> Dict[str, Any]:
        if context is None:
            context = {}
            
        # Get product type
        product_type = source_data.get('type_id', 'simple')
        
        # Initialize result
        result = {
            "status": "draft",
            "metadata": {},
            "images": [],
            "tags": [],
            # "collection_id": "col_xxx"
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
        
        # Apply option fallback
        self._apply_option_fallback(result)
        
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
            
        self._map_special_fields(result, source_data)
        
    def _map_special_fields(self, result: Dict, source_data: Dict):
        description = None
        
        if 'custom_attributes' in source_data:
            for attr in source_data['custom_attributes']:
                if attr.get('attribute_code') == 'description':
                    description = attr.get('value')
                    break
        
        if not description:
            description = source_data.get('description')
        
        if description:
            clean_description = self.transformer.html_to_text(description)
            # clean_description = self.transformer.clean_html(description)
            
            result['description'] = clean_description
        
        # Map short_description/subtitle
        short_description = None
        
        if 'custom_attributes' in source_data:
            for attr in source_data['custom_attributes']:
                if attr.get('attribute_code') == 'short_description':
                    short_description = attr.get('value')
                    break
        
        if not short_description:
            short_description = source_data.get('short_description')
        
        if short_description:
            clean_short_desc = self.transformer.html_to_text(short_description)
            result['subtitle'] = clean_short_desc
    
    def _apply_field_transformation(self, value: Any, transform: str, 
                                   field_name: str, source_data: Dict, 
                                   context: Dict) -> Any:
        try:
            if transform == 'clean_sku':
                return self.transformer.clean_sku(value)
            elif transform == 'strip':
                return self.transformer.strip(value)
            elif transform == 'html_to_text':
                return html_to_text(value)
            elif transform == 'clean_html':
                return self.transformer.clean_html(value)
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
        
        option_values_by_type = {}
        
        special_attributes = ['description', 'short_description', 'meta_title', 'meta_keywords', 'meta_description']
        
        for attr in custom_attrs:
            attr_code = attr.get('attribute_code')
            attr_value = attr.get('value')
            
            if attr_code and attr_value is not None:
                if attr_code in special_attributes:
                    continue    
                if attr_code in ['color', 'size', 'material', 'brand']:
                    if 'metadata' not in result:
                        result['metadata'] = {}
                    result['metadata'][attr_code] = attr_value
                    
                    if attr_code not in option_values_by_type:
                        option_values_by_type[attr_code] = []
                    
                    value_str = str(attr_value)
                    if value_str not in option_values_by_type[attr_code]:
                        option_values_by_type[attr_code].append(value_str)
                else:
                    metadata_key = f"magento_{attr_code}"
                    if 'metadata' not in result:
                        result['metadata'] = {}
                    result['metadata'][metadata_key] = attr_value
        
        if option_values_by_type:
            options = []
            for attr_code, values in option_values_by_type.items():
                if values:  
                    options.append({
                        'title': attr_code.capitalize(),
                        'values': values
                    })
            
            if options:
                result['options'] = options
    
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
    
    def _apply_option_fallback(self, result: Dict[str, Any]):
        """Apply fallback options when product has no options"""
        fallback = self.get_fallback_options()
        logger.info(f"Applying option fallback with config: {fallback}")
        
        logger.info(f"_apply_option_fallback called for product: {result.get('title')}")
        logger.info(f"Fallback enabled: {fallback.get('enabled', False)}")
        logger.info(f"Current options in result: {result.get('options')}")
        
        # Kiểm tra kỹ hơn
        if not fallback:
            logger.warning("No fallback_options configuration found!")
            return
            
        if not fallback.get("enabled", False):
            logger.info("Fallback options not enabled")
            return

        # Kiểm tra xem product đã có options chưa
        existing_options = result.get("options")
        
        # DEBUG: Kiểm tra logic
        logger.info(f"Existing options check: {existing_options}")
        logger.info(f"Type of existing_options: {type(existing_options)}")
        
        # Sửa logic kiểm tra: Chỉ bỏ qua nếu existing_options là list khác rỗng
        if isinstance(existing_options, list) and len(existing_options) > 0:
            logger.info(f"Product already has {len(existing_options)} options, skipping fallback")
            return
        
        # Nếu existing_options là None hoặc list rỗng, apply fallback
        logger.info("Product has no options or empty options list, applying fallback...")
        
        fallback_values = fallback.get("values", [])
        logger.info(f"Fallback values: {fallback_values}")
        
        if not fallback_values:
            logger.warning("Fallback options enabled but no values configured")
            # Tạo fallback mặc định nếu config trống
            fallback_values = [{
                "title": "Default",
                "values": ["Default"]
            }]
            logger.info(f"Using default fallback: {fallback_values}")
        
        result["options"] = [
            {
                "title": opt["title"],
                "values": opt.get("values", ["Default"])  # Default nếu values trống
            }
            for opt in fallback_values
        ]
        
        logger.info(f"Applied fallback options: {result['options']}")
        
        # Cập nhật variants với fallback values
        if result.get('variants'):
            for variant in result['variants']:
                variant_options = {}
                for option in result["options"]:
                    title = option["title"]
                    values = option.get("values", [])
                    if values:
                        variant_options[title] = values[0]
                    else:
                        variant_options[title] = "Default"
                variant['options'] = variant_options
                logger.info(f"Updated variant {variant.get('sku')} with options: {variant_options}")
        
        logger.info("Successfully applied fallback options")

    def _process_simple_product(self, result: Dict, source_data: Dict):
        if 'variants' not in result:
            result['variants'] = []
        
        variant = {
            'title': result.get('title', 'Default Variant'),
            'sku': source_data.get('sku', ''),
            'manage_inventory': True,
            'allow_backorder': False,
            # 'inventory_quantity': int(source_data.get('quantity', 0)),
            'prices': self._build_prices(source_data),
            'options': {},
            # 'metadata': {}
        }
        
        # Add weight if available
        weight = source_data.get('weight')
        if weight:
            variant['weight'] = float(weight)
        
        result['variants'].append(variant)
        self._create_product_options_from_metadata(result, source_data)
        
    def _create_product_options_from_metadata(self, result: Dict, source_data: Dict):
        metadata = result.get('metadata', {})
        options = []
        
        option_fields = ['size', 'color', 'material', 'brand']
        
        for field in option_fields:
            if field in metadata:
                value = metadata[field]
                if isinstance(value, str):
                    values = [value]
                elif isinstance(value, list):
                    values = value
                else:
                    values = [str(value)]
                
                options.append({
                    'title': field.capitalize(),
                    'values': values
                })
        
        if options:
            result['options'] = options
            
            if result.get('variants'):
                for variant in result['variants']:
                    variant_options = {}
                    for option in options:
                        title = option['title']
                        if option['values']:
                            # Lấy giá trị đầu tiên làm default
                            variant_options[title] = option['values'][0]
                    variant['options'] = variant_options
    
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
    
    def _build_variant_options(self, option_values: Dict[str, str], product_options: List[Dict]) -> Dict[str, str]:
        result = {}
        for option in product_options:
            title = option["title"]
            if title in option_values:
                result[title] = option_values[title]
            else:
                # Fallback to first value if available
                if option.get("values"):
                    result[title] = option["values"][0]
        return result
    
    def _build_variant(self, child: Dict, parent_options: List[Dict]) -> Optional[Dict]:
        if not child:
            return None

        variant = {
            "title": child.get("name", "Variant"),
            "sku": child.get("sku", ""),
            "manage_inventory": True,
            "allow_backorder": False,
            "prices": self._build_prices(child),
            "metadata": {}
        }

        weight = child.get("weight")
        if weight:
            variant["weight"] = float(weight)
        if not parent_options:
            variant["options"] = {} 
            return variant
        
        options_object: Dict[str, str] = {}
        # option_values_by_title: Dict[str, str] = {}

        for attr in child.get("custom_attributes", []):
            attr_code = attr.get("attribute_code")
            attr_value = str(attr.get("value"))

            for parent_opt in parent_options:
                if parent_opt["title"].lower() == attr_code.lower():
                    display_value = self.value_maps.get(attr_code, {}).get(attr_value, attr_value)
                    options_object[parent_opt["title"]] = display_value
                    break

        missing_options = []
        for parent_opt in parent_options:
            title = parent_opt["title"]
            if title not in options_object:
                missing_options.append(title)
                
        if missing_options:
            logger.warning(f"Missing options {missing_options} for variant SKU={child.get('sku')}")
            return None

        variant["options"] = options_object
        return variant

    def _build_prices(self, product: Dict) -> List[Dict]:
        price = product.get('price', 0)
        currency = product.get('currency', 'usd').lower()
        try:
            amount = self.transformer.normalize_price(price)
        except Exception:
            amount = 0
        
        return [{
            'currency_code': currency,
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
        # quantity = variant_data.get('quantity', 0)
        # if quantity is not None:
        #     result['inventory_quantity'] = int(quantity)
        
        # Add weight
        weight = variant_data.get('weight')
        if weight:
            result['weight'] = float(weight)
        
        # Map variant options if parent provided
        if parent_product and 'options' in parent_product:
            # This would map variant-specific options
            pass
        
        return result