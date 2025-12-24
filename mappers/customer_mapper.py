from typing import Any, Dict, Optional, List
from mappers.base_mapper import BaseMapper
from core.transformer import Transformer
from core.validator import Validator
from utils.logger import logger
import re
from datetime import datetime
import pycountry


class CustomerMapper(BaseMapper):
    """Mapper for Magento to Medusa customer conversion"""
    
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
        
    def map(self, source_data: Dict[str, Any], context: Optional[Dict] = None) -> Dict[str, Any]:
        if context is None:
            context = {}
            
        # Initialize result
        result = {
            "metadata": {},
            "billing_address": {},
            "shipping_address": {}
        }
        
        try:
            # Map basic fields using config
            self._map_fields_with_config(result, source_data)
            
            # Map addresses
            self._map_addresses_with_config(result, source_data)
            
            # Map custom attributes
            self._map_custom_attributes(result, source_data)
            
            # Add metadata
            result = self._add_metadata(result, source_data, context)
            
            # Apply custom transformations
            result = self._apply_custom_transformations(result, source_data)
            
        except Exception as e:
            logger.error(f"Error mapping customer: {e}")
            raise
            
        return result
    
    def _map_fields_with_config(self, result: Dict, source_data: Dict):
        """Map fields using YAML configuration"""
        fields_config = self.config.get('fields', {})
        
        for source_field, field_config in fields_config.items():
            if not isinstance(field_config, dict):
                continue
                
            # Skip custom attributes field (handled separately)
            if source_field == 'custom_attributes':
                continue
                
            target_field = field_config.get('target')
            if target_field is None:
                continue
                
            # Get source value from multiple possible paths
            source_value = self._get_source_value(source_data, source_field, field_config)
            
            # Apply transformation if specified
            transform = field_config.get('transform')
            if transform and source_value is not None:
                transformed_value = self.transformer.transform(source_value, transform)
            else:
                transformed_value = source_value
                
            # Set default value if empty
            if transformed_value is None and 'default' in field_config:
                transformed_value = field_config['default']
                
            # Set value if not None
            if transformed_value is not None:
                self._set_nested_value(result, target_field, transformed_value)
    
    def _get_source_value(self, source_data: Dict, source_field: str, field_config: Dict) -> Any:
        """Get value from source data, checking multiple possible paths"""
        # Check for source_paths first
        source_paths = field_config.get('source_paths', [])
        if source_paths:
            for path in source_paths:
                value = self._get_nested_value(source_data, path)
                if value is not None and value != '':
                    return value
        
        # Check for single source_path
        source_path = field_config.get('source_path', source_field)
        return self._get_nested_value(source_data, source_path)
    
    def _map_addresses_with_config(self, result: Dict, source_data: Dict):
        """Map customer addresses using configuration"""
        addresses = source_data.get('addresses', [])
        
        if not addresses:
            return
            
        # Get address configuration
        address_fields_config = self.config.get('address_fields', {})
        address_rules_config = self.config.get('address_rules', {})
        
        # Process shipping address
        shipping_address = self._select_address_by_priority(
            addresses, 
            address_rules_config.get('shipping', {}).get('priority', [])
        )
        
        if shipping_address:
            result['shipping_address'] = self._map_single_address(
                shipping_address, 
                address_fields_config
            )
        
        # Process billing address
        billing_address = self._select_address_by_priority(
            addresses,
            address_rules_config.get('billing', {}).get('priority', [])
        )
        
        if billing_address:
            result['billing_address'] = self._map_single_address(
                billing_address,
                address_fields_config
            )
        
        # Fallback: if no addresses found, use first address for both
        if not result.get('shipping_address') and not result.get('billing_address') and addresses:
            first_address = addresses[0]
            mapped_address = self._map_single_address(first_address, address_fields_config)
            result['shipping_address'] = mapped_address
            result['billing_address'] = mapped_address
        elif not result.get('shipping_address') and result.get('billing_address'):
            result['shipping_address'] = result['billing_address']
        elif not result.get('billing_address') and result.get('shipping_address'):
            result['billing_address'] = result['shipping_address']
    
    def _select_address_by_priority(self, addresses: List[Dict], priority_rules: List[str]) -> Optional[Dict]:
        """Select address based on priority rules"""
        if not addresses or not priority_rules:
            return None
            
        for rule in priority_rules:
            for address in addresses:
                if rule == 'first_address':
                    return address
                elif rule == 'is_default_shipping' and address.get('is_default_shipping'):
                    return address
                elif rule == 'is_default_billing' and address.get('is_default_billing'):
                    return address
                    
        return addresses[0] if addresses else None
    
    def _map_single_address(self, address: Dict, address_fields_config: Dict) -> Dict:
        """Map single address using configuration"""
        result = {}
        
        for address_field, field_config in address_fields_config.items():
            if not isinstance(field_config, dict):
                continue
                
            # Get target field(s)
            target_fields = field_config.get('target')
            if isinstance(target_fields, str):
                target_fields = [target_fields]
            elif not isinstance(target_fields, list):
                continue
                
            # Get source value
            source_path = field_config.get('source_path', address_field)
            source_value = self._get_nested_value(address, source_path)
            
            # Apply transformation
            transform = field_config.get('transform')
            if transform and source_value is not None:
                transformed_value = self.transformer.transform(source_value, transform)
            else:
                transformed_value = source_value
                
            # Handle array values (like street splitting)
            if isinstance(transformed_value, list) and len(target_fields) == len(transformed_value):
                for i, target_field in enumerate(target_fields):
                    self._set_nested_value(result, target_field, transformed_value[i])
            elif transformed_value is not None:
                for target_field in target_fields:
                    self._set_nested_value(result, target_field, transformed_value)
                    
        return result
    
    def _map_custom_attributes(self, result: Dict, source_data: Dict):
        """Map custom attributes to metadata"""
        custom_attrs = source_data.get('custom_attributes', [])
        
        for attr in custom_attrs:
            attr_code = attr.get('attribute_code')
            attr_value = attr.get('value')
            
            if attr_code and attr_value is not None:
                # Map common attributes to specific fields
                if attr_code == 'gender':
                    result['metadata']['gender'] = attr_value
                elif attr_code == 'dob':
                    result['date_of_birth'] = attr_value
                elif attr_code == 'taxvat':
                    result['tax_number'] = attr_value
                else:
                    # Map to metadata with prefix
                    metadata_key = f"magento_{attr_code}"
                    result['metadata'][metadata_key] = attr_value
    
    def _apply_custom_transformations(self, result: Dict, source_data: Dict) -> Dict:
        """Apply custom transformations defined in config"""
        transformations_config = self.config.get('transformations', {})
        
        for trans_name, trans_rule in transformations_config.items():
            if trans_name == 'concat' and 'full_name' not in result:
                firstname = source_data.get('firstname', '')
                lastname = source_data.get('lastname', '')
                if firstname or lastname:
                    result['full_name'] = f"{firstname} {lastname}".strip()
                    
        return result
    
    # Helper methods (keep from original)
    def _clean_phone(self, phone: str) -> str:
        """Clean phone number"""
        if not phone:
            return ""
        
        # Remove non-numeric characters except +
        cleaned = re.sub(r'[^\d+]', '', phone)
        
        # Ensure it starts with +
        if not cleaned.startswith('+'):
            # Assume US number if no country code
            if len(cleaned) == 10:
                cleaned = f"+1{cleaned}"
            elif len(cleaned) == 11 and cleaned.startswith('1'):
                cleaned = f"+{cleaned}"
        
        return cleaned
    
    def _map_country_code(self, magento_country_id: str) -> str:
        if not magento_country_id:
            return None
        
        # country_map = {
        #     'US': 'us', 'GB': 'gb', 'CA': 'ca', 'AU': 'au',
        #     'DE': 'de', 'FR': 'fr', 'IT': 'it', 'ES': 'es',
        #     'JP': 'jp', 'CN': 'cn', 'IN': 'in', 'BR': 'br',
        #     'MX': 'mx',
        # }
        
        country = pycountry.countries.get(alpha_2=magento_country_id.upper())
        return country.alpha_2.lower() if country else magento_country_id.lower()
    
    def _add_metadata(self, result: Dict, source_data: Dict, context: Dict) -> Dict:
        """Add additional metadata"""
        # Add source system info
        result['metadata']['source_system'] = self.config.get('source', 'magento')
        result['metadata']['source_id'] = source_data.get('id')
        
        # Add group/customer segment if available
        group_id = source_data.get('group_id')
        if group_id:
            result['metadata']['customer_group'] = group_id
        
        # Add store info
        store_id = source_data.get('store_id')
        if store_id:
            result['metadata']['store_id'] = store_id
        
        # Add sync info
        result['metadata']['sync_timestamp'] = context.get('sync_timestamp', datetime.now().isoformat())
        
        return result
    
    def _get_nested_value(self, data: Dict, path: str, default: Any = None) -> Any:
        """Get value from nested dictionary using dot notation"""
        if not path:
            return default
            
        keys = path.split('.')
        value = data
        
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key, default)
                if value is default:
                    break
            else:
                return default
                
        return value
    
    def _set_nested_value(self, data: Dict, path: str, value: Any):
        """Set value in nested dictionary using dot notation"""
        if not path:
            return
            
        keys = path.split('.')
        current = data
        
        for i, key in enumerate(keys[:-1]):
            if key not in current:
                current[key] = {}
            current = current[key]
            
        current[keys[-1]] = value
    
    def validate_customer(self, customer_data: Dict) -> Dict[str, List[str]]:
        """Validate customer data"""
        errors = {
            "missing_required": [],
            "invalid_format": []
        }
        
        # Check required fields from config
        required_fields = self.config.get('validation', {}).get('required_fields', ['email', 'firstname', 'lastname'])
        for field in required_fields:
            if field not in customer_data or not customer_data[field]:
                errors["missing_required"].append(field)
        
        # Validate email format
        email = customer_data.get('email', '')
        if email and not self._is_valid_email(email):
            errors["invalid_format"].append('email')
        
        # Validate phone if present
        phone = customer_data.get('telephone') or customer_data.get('phone', '')
        if phone and not self._is_valid_phone(phone):
            errors["invalid_format"].append('phone')
        
        return errors
    
    def _is_valid_email(self, email: str) -> bool:
        """Validate email format"""
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return bool(re.match(pattern, email))
    
    def _is_valid_phone(self, phone: str) -> bool:
        """Validate phone format"""
        # Remove all non-digit characters except +
        cleaned = re.sub(r'[^\d+]', '', phone)
        return len(cleaned) >= 10 and len(cleaned) <= 15