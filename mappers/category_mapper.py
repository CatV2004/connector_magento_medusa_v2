from typing import Any, Dict, Optional
from mappers.base_mapper import BaseMapper
from core.transformer import Transformer
from core.validator import Validator
from utils.logger import logger


class CategoryMapper(BaseMapper):
    """Mapper for Magento to Medusa category conversion"""
    
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
        self.id_mapping = {}  # Magento ID -> Medusa ID
        
    def map(self, source_data: Dict[str, Any], context: Optional[Dict] = None) -> Dict[str, Any]:
        # Set default context
        if context is None:
            context = {}
        
        if not isinstance(source_data, dict):
            raise TypeError(
                f"source_data must be dict, got {type(source_data)}"
            )
            
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
    
    def _apply_custom_transformations(self, result: Dict, source_data: Dict, context: Dict):
        custom_transforms = self.mapping_config.get('transformations', {})

        for target_field, rule_obj in custom_transforms.items():
            # rule_obj có thể là dict hoặc trực tiếp string
            rule = None
            if isinstance(rule_obj, dict):
                rule = rule_obj.get('rule')
            elif isinstance(rule_obj, str):
                rule = rule_obj

            if not isinstance(rule, str):
                logger.warning(f"Invalid custom transform for {target_field}: {rule_obj}")
                continue  # skip invalid rule

            try:
                # Handle if_empty
                if rule.startswith('if_empty(') and (target_field not in result or not result[target_field]):
                    func_call = rule[len('if_empty('):-1]  # remove wrapper
                    if '(' in func_call and func_call.endswith(')'):
                        func_name, arg_name = func_call.split('(')
                        arg_name = arg_name[:-1]  # remove closing ')'
                        arg_value = source_data.get(arg_name, '')
                        if hasattr(self.transformer, func_name):
                            result[target_field] = getattr(self.transformer, func_name)(arg_value)
                        else:
                            logger.warning(f"Transformer has no method '{func_name}'")
                    else:
                        logger.warning(f"Invalid if_empty syntax for {target_field}: {rule}")

                # Handle not(...)
                elif rule.startswith('not(') and rule.endswith(')'):
                    arg_name = rule[len('not('):-1]
                    arg_value = source_data.get(arg_name, False)
                    result[target_field] = not bool(arg_value)

                # Handle if(...) - simplified
                elif rule.startswith('if(') and rule.endswith(')'):
                    # parse simple "if(condition, true_val, false_val)"
                    # You can implement a safe eval or custom parser here
                    pass

                # else: unknown rule
                else:
                    logger.warning(f"Unknown transformation rule for {target_field}: {rule}")

            except Exception as e:
                logger.error(f"Custom transformation failed for {target_field} with rule {rule}: {e}")
    
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