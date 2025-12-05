from typing import Any, Dict, Optional
from mappers.base_mapper import BaseMapper
from core.transformer import Transformer
from core.validator import Validator
from utils.logger import logger


class CategoryMapper(BaseMapper):
    """Mapper for Magento to Medusa category conversion"""
    
    def __init__(self, source_connector=None, target_connector=None):
        super().__init__('category_mapping.yaml', source_connector, target_connector)
        self.transformer = Transformer()
        self.validator = Validator()
        self.id_mapping = {}  # Magento ID -> Medusa ID
        
    def map(self, source_data: Dict[str, Any], context: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Map Magento category to Medusa format
        
        Args:
            source_data: Magento category data
            context: Additional context (e.g., parent mapping, store info)
            
        Returns:
            Dict: Medusa formatted category
        """
        # Set default context
        if context is None:
            context = {}
            
        # Update context with ID mapping if available
        if 'id_mapping' not in context:
            context['id_mapping'] = self.id_mapping
            
        # Validate source data
        validation_errors = self.validate(source_data)
        if validation_errors['missing_required']:
            logger.warning(f"Category {source_data.get('id')} missing required fields: "
                          f"{validation_errors['missing_required']}")
            
        # Initialize result
        result = {}
        fields_config = self.mapping_config.get('fields', {})
        
        # Process each field according to mapping config
        for source_field, field_config in fields_config.items():
            if source_field not in source_data:
                continue
                
            target_field = field_config.get('target')
            if not target_field:
                continue  # Field not mapped to target
                
            source_value = source_data[source_field]
            field_type = field_config.get('type', 'string')
            required = field_config.get('required', False)
            default = field_config.get('default')
            transform = field_config.get('transform')
            
            # Apply transformation if specified
            if transform:
                transformed_value = self._apply_transformation(
                    source_value, transform, source_field, context
                )
            else:
                transformed_value = source_value
                
            # Set value in result (handle nested fields)
            self._set_nested_value(result, target_field, transformed_value)
            
        # Apply custom transformations from config
        self._apply_custom_transformations(result, source_data, context)
        
        # Add metadata
        result = self._add_metadata(result, source_data)
        
        return result
    
    def _apply_transformation(self, value: Any, transform: str, 
                             source_field: str, context: Dict) -> Any:
        """Apply transformation to value"""
        try:
            if transform == 'strip':
                return self.transformer.strip(value)
            elif transform == 'html_to_text':
                return self.transformer.html_to_text(value)
            elif transform == 'slugify':
                return self.transformer.slugify(value)
            elif transform == 'invert_boolean':
                return not bool(value)
            elif transform == 'map_parent_id':
                return self._map_parent_id(value, context)
            elif transform.startswith('if_empty('):
                # Extract function name from if_empty(func_name)
                func_name = transform[9:-1]
                if not value:
                    return getattr(self.transformer, func_name)(context.get('default_value', ''))
                return value
            else:
                # Try to call transformer method
                if hasattr(self.transformer, transform):
                    return getattr(self.transformer, transform)(value)
                return value
        except Exception as e:
            logger.error(f"Transformation failed for {source_field}: {e}")
            return value
            
    def _map_parent_id(self, parent_id: Any, context: Dict) -> Optional[str]:
        """Map Magento parent ID to Medusa parent ID"""
        if not parent_id or parent_id in [1, 2]:  # Root categories
            return None
            
        id_mapping = context.get('id_mapping', {})
        magento_id_str = str(parent_id)
        
        if magento_id_str in id_mapping:
            return id_mapping[magento_id_str]
            
        # Parent not yet mapped - this will be handled in sync service
        return None
    
    def _apply_custom_transformations(self, result: Dict, 
                                     source_data: Dict, context: Dict):
        """Apply custom transformations defined in mapping config"""
        custom_transforms = self.mapping_config.get('transformations', {})
        
        for target_field, rule in custom_transforms.items():
            if rule.startswith('if_empty('):
                # Example: if_empty(slugify(name))
                func_call = rule[9:-1]  # Remove if_empty()
                if target_field not in result or not result[target_field]:
                    # Extract function name (e.g., 'slugify(name)')
                    if '(' in func_call:
                        func_name = func_call.split('(')[0]
                        arg = func_call.split('(')[1][:-1]  # Remove closing )
                        value = source_data.get(arg, '')
                        result[target_field] = getattr(self.transformer, func_name)(value)
            elif rule.startswith('not('):
                # Example: not(include_in_menu)
                arg = rule[4:-1]  # Remove not()
                value = source_data.get(arg, False)
                result[target_field] = not bool(value)
            elif rule.startswith('if('):
                # Example: if(parent_id > 2, map_id(parent_id), null)
                # This is simplified - in real implementation would parse properly
                pass
    
    def _set_nested_value(self, obj: Dict, path: str, value: Any):
        """Set value for nested field path (e.g., 'metadata.position')"""
        if '.' not in path:
            obj[path] = value
            return
            
        parts = path.split('.')
        current = obj
        for part in parts[:-1]:
            if part not in current:
                current[part] = {}
            current = current[part]
        current[parts[-1]] = value
    
    def _add_metadata(self, result: Dict, source_data: Dict) -> Dict:
        """Add additional metadata to result"""
        if 'metadata' not in result:
            result['metadata'] = {}
            
        # Add source system metadata
        result['metadata']['source_system'] = self.source_system
        result['metadata']['source_id'] = source_data.get('id')
        result['metadata']['source_parent_id'] = source_data.get('parent_id')
        
        # Add timestamps
        if 'created_at' in source_data:
            result['metadata']['source_created_at'] = source_data['created_at']
        if 'updated_at' in source_data:
            result['metadata']['source_updated_at'] = source_data['updated_at']
            
        return result
    
    def update_id_mapping(self, magento_id: str, medusa_id: str):
        """Update ID mapping for parent-child relationships"""
        self.id_mapping[magento_id] = medusa_id
        
    def get_id_mapping(self) -> Dict[str, str]:
        """Get current ID mapping"""
        return self.id_mapping.copy()