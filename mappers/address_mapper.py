from typing import Any, Dict, Optional, List
from dataclasses import dataclass, field
from enum import Enum
from mappers.base_mapper import BaseMapper
from core.transformer import Transformer
from core.validator import Validator
from utils.logger import logger
import re
import pycountry


class AddressType(Enum):
    SHIPPING = "shipping"
    BILLING = "billing"
    BOTH = "both"
    GENERAL = "general"


@dataclass
class AddressMappingResult:
    address_data: Dict[str, Any] = field(default_factory=dict)
    address_type: AddressType = AddressType.GENERAL
    validation_errors: Dict[str, list] = field(default_factory=dict)
    is_primary: bool = False


class AddressMapper(BaseMapper):
    
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
        
    def map(self, source_data: Dict[str, Any], context: Optional[Dict] = None) -> AddressMappingResult:
        if context is None:
            context = {}
            
        result = AddressMappingResult()
        
        try:
            # Step 1: Determine address type
            result.address_type = self._determine_address_type(source_data, context)
            result.is_primary = self._is_primary_address(source_data)
            
            # Step 2: Validate input
            result.validation_errors = self.validate(source_data)
            if result.validation_errors:
                logger.warning(f"Address validation errors: {result.validation_errors}")
            
            # Step 3: Map basic address fields
            self._map_basic_fields(result, source_data, context)
            
            # Step 4: Map address-specific fields
            self._map_address_specific_fields(result, source_data)
            
            # Step 5: Add address metadata
            self._add_metadata(result, source_data, context)
            
            # Step 6: Format Medusa-specific fields
            self._format_medusa_fields(result.address_data)
            
            # Step 7: Generate address name
            self._generate_address_name(result)
            
            # Step 8: Validate output format
            self._validate_output_format(result.address_data)
            
        except Exception as e:
            logger.error(f"Error mapping address: {e}")
            result.validation_errors.setdefault('mapping_errors', []).append(str(e))
            raise
            
        return result
    
    def validate(self, source_data: Dict[str, Any]) -> Dict[str, list]:
        """
        Validate address data before mapping.
        
        Returns:
            Dictionary of validation errors by category
        """
        errors = {
            "missing_required": [],
            "invalid_format": [],
            "geographical": []
        }
        
        required_fields = self.mapping_config.get('validation', {}).get('required_fields', [
            'street', 'city', 'country_id', 'postcode'
        ])
        
        # Check required fields
        for field_name in required_fields:
            value = self._get_nested_value(source_data, field_name)
            if not value or (isinstance(value, str) and value.strip() == ''):
                errors["missing_required"].append(field_name)
        
        # Validate country code
        country_id = source_data.get('country_id')
        if country_id and not self._is_valid_country_code(country_id):
            errors["geographical"].append(f"Invalid country code: {country_id}")
        
        # Validate postal code format based on country
        postcode = source_data.get('postcode')
        if postcode and country_id:
            if not self._is_valid_postal_code(postcode, country_id):
                errors["invalid_format"].append(f"Invalid postal code for {country_id}: {postcode}")
        
        # Clean up empty error categories
        return {k: v for k, v in errors.items() if v}
    
    def _determine_address_type(self, source_data: Dict, context: Dict) -> AddressType:
        is_default_shipping = source_data.get('is_default_shipping', False)
        is_default_billing = source_data.get('is_default_billing', False)
        
        # Context can override (e.g., when processing shipping addresses only)
        context_type = context.get('address_type')
        if context_type:
            return AddressType(context_type)
        
        # Determine based on flags
        if is_default_shipping and is_default_billing:
            return AddressType.BOTH
        elif is_default_shipping:
            return AddressType.SHIPPING
        elif is_default_billing:
            return AddressType.BILLING
        else:
            return AddressType.GENERAL
    
    def _is_primary_address(self, source_data: Dict) -> bool:
        """Check if this is a primary address"""
        return any([
            source_data.get('is_default_shipping', False),
            source_data.get('is_default_billing', False),
            source_data.get('default_shipping', False),
            source_data.get('default_billing', False)
        ])
    
    def _map_basic_fields(self, result: AddressMappingResult, source_data: Dict, context: Dict):
        """Map basic address fields from config"""
        fields_config = self.mapping_config.get('fields', {})
        
        for source_field, field_config in fields_config.items():
            if not isinstance(field_config, dict):
                continue
                
            target_field = field_config.get('target')
            if not target_field:
                continue
            
            # Get source value
            source_value = self._get_source_value(source_data, source_field, field_config)
            
            # Apply transformation if specified
            if field_config.get('transform') and source_value is not None:
                transformed_value = self.transformer.transform(
                    source_value, 
                    field_config['transform']
                )
            else:
                transformed_value = source_value
            
            # Apply default value if source is None
            if transformed_value is None and 'default' in field_config:
                transformed_value = field_config['default']
            
            # Handle array targets (e.g., street -> [address_1, address_2])
            if isinstance(target_field, list) and transformed_value is not None:
                if isinstance(transformed_value, list):
                    # Map array to array
                    for i, target in enumerate(target_field):
                        if i < len(transformed_value):
                            self._set_nested_value(
                                result.address_data, 
                                target, 
                                transformed_value[i]
                            )
                else:
                    # Single value to first target, empty for others
                    for i, target in enumerate(target_field):
                        value = transformed_value if i == 0 else ''
                        self._set_nested_value(result.address_data, target, value)
            elif transformed_value is not None:
                # Single target field
                self._set_nested_value(
                    result.address_data, 
                    target_field, 
                    transformed_value
                )
    
    def _get_source_value(self, source_data: Dict, source_field: str, field_config: Dict) -> Any:
        """Get value from source data with multiple possible paths"""
        # Check source_paths first
        source_paths = field_config.get('source_paths', [])
        for path in source_paths:
            value = self._get_nested_value(source_data, path)
            if value is not None and value != '':
                return value
        
        # Check source_path or default to source_field
        source_path = field_config.get('source_path', source_field)
        return self._get_nested_value(source_data, source_path)
    
    def _map_address_specific_fields(self, result: AddressMappingResult, source_data: Dict):
        is_default_shipping = any([
            source_data.get('is_default_shipping', False),
            source_data.get('default_shipping', False)
        ])
        
        is_default_billing = any([
            source_data.get('is_default_billing', False),
            source_data.get('default_billing', False)
        ])
        
        result.address_data['is_default_shipping'] = is_default_shipping
        result.address_data['is_default_billing'] = is_default_billing
        
        # Map region data - CHỈ lấy province, không tạo province_code/province_name
        if region := source_data.get('region'):
            if isinstance(region, dict):
                # Ưu tiên region_code nếu có, sau đó là region name
                region_code = region.get('region_code')
                region_name = region.get('region')
                
                # Sử dụng region_code nếu có, nếu không dùng region_name
                province_value = region_code or region_name
                
                if province_value:
                    result.address_data['province'] = province_value
            elif isinstance(region, str):
                result.address_data['province'] = region
        
        # XÓA các field không hợp lệ nếu đã vô tình tạo ra
        result.address_data.pop('province_code', None)
        result.address_data.pop('province_name', None)
    
    def _add_metadata(self, result: AddressMappingResult, source_data: Dict, context: Dict):
        """Add metadata to address data"""
        metadata = result.address_data.setdefault('metadata', {})
        
        # Source system info
        metadata['source_system'] = self.mapping_config.get('source', 'magento')
        metadata['source_address_id'] = source_data.get('id')
        
        # Address type metadata
        metadata['address_type'] = result.address_type.value
        metadata['is_primary'] = result.is_primary
        
        # Magento-specific metadata
        if customer_id := source_data.get('customer_id'):
            metadata['source_customer_id'] = customer_id
        
        if telephone := source_data.get('telephone'):
            metadata['source_phone'] = telephone
        
        # Sync context
        metadata['sync_timestamp'] = context.get('sync_timestamp')
        metadata['customer_id'] = context.get('customer_id')
        
        # Geographical metadata
        if country_id := source_data.get('country_id'):
            metadata['source_country_code'] = country_id
        
        if region_id := source_data.get('region_id'):
            metadata['source_region_id'] = region_id
    
    def _format_medusa_fields(self, address_data: Dict):
        address_data.pop('province_code', None)
        address_data.pop('province_name', None)
        
        # Format province code (country-province format)
        country_code = address_data.get('country_code', '').lower()
        province = address_data.get('province')
        
        # Chỉ format province nếu có cả country_code và province
        if country_code and province:
            # Đảm bảo province không chứa country code prefix
            if '-' in province:
                # Nếu đã có dạng country-province, chỉ lấy phần province
                province_part = province.split('-')[-1]
            else:
                province_part = province
            
            # Format province code
            formatted_province = self._format_province_code(province_part)
            address_data['province'] = f"{country_code}-{formatted_province}"
        
        # Ensure country code is lowercase
        if 'country_code' in address_data:
            address_data['country_code'] = address_data['country_code'].lower()
        
        # Clean phone number
        if phone := address_data.get('phone'):
            address_data['phone'] = self._clean_phone_number(phone)
    
    def _format_province_code(self, province: str) -> str:
        """Format province code for Medusa (us-ca format)"""
        if not province:
            return ""
        
        province = province.lower()
        
        formatted = re.sub(r'[^a-z0-9\-]', '', province)
        
        formatted = re.sub(r'-+', '-', formatted)
        
        formatted = formatted.strip('-')
        
        if not formatted:
            formatted = "unknown"
        
        return formatted
    
    def _generate_address_name(self, result: AddressMappingResult):
        address_type = result.address_type
        is_primary = result.is_primary
        
        address_names = {
            (AddressType.SHIPPING, True): "Default Shipping Address",
            (AddressType.BILLING, True): "Default Billing Address",
            (AddressType.BOTH, True): "Default Address",
            (AddressType.SHIPPING, False): "Shipping Address",
            (AddressType.BILLING, False): "Billing Address",
            (AddressType.GENERAL, True): "Primary Address",
            (AddressType.GENERAL, False): "Address"
        }
        
        address_name = address_names.get(
            (address_type, is_primary), 
            f"{address_type.value.title()} Address"
        )
        
        # Add location hint if available
        if city := result.address_data.get('city'):
            address_name = f"{address_name} - {city}"
        
        result.address_data['address_name'] = address_name
    
    def _validate_output_format(self, address_data: Dict):
        required_fields = ['address_1', 'city', 'country_code', 'postal_code']
        
        for field in required_fields:
            if not address_data.get(field):
                raise ValueError(f"Missing required address field: {field}")
        
        # Validate province format
        if province := address_data.get('province'):
            if '-' not in province:
                logger.warning(f"Province code might not be in Medusa format: {province}")
        
        country_code = address_data.get('country_code', '').upper()
        if not self._is_valid_country_code(country_code):
            raise ValueError(f"Invalid country code in output: {country_code}")
    
    def _is_valid_country_code(self, country_code: str) -> bool:
        if not country_code:
            return False
        
        try:
            # Check ISO 3166-1 alpha-2
            country = pycountry.countries.get(alpha_2=country_code.upper())
            return country is not None
        except (KeyError, AttributeError):
            # Fallback to common country codes
            common_codes = {'US', 'GB', 'CA', 'AU', 'DE', 'FR', 'IT', 'ES', 'JP', 'CN', 'IN', 'BR', 'MX'}
            return country_code.upper() in common_codes
    
    def _is_valid_postal_code(self, postcode: str, country_id: str) -> bool:
        """Validate postal code based on country"""
        if not postcode or not country_id:
            return True  # Can't validate without both
        
        country_code = country_id.upper()
        
        # Simple regex patterns for common countries
        patterns = {
            'US': r'^\d{5}(-\d{4})?$',
            'CA': r'^[A-Za-z]\d[A-Za-z] \d[A-Za-z]\d$',
            'GB': r'^[A-Z]{1,2}\d[A-Z\d]? ?\d[A-Z]{2}$',
            'AU': r'^\d{4}$',
            'DE': r'^\d{5}$',
            'FR': r'^\d{5}$',
            'IT': r'^\d{5}$',
            'ES': r'^\d{5}$',
            'JP': r'^\d{3}-\d{4}$',
        }
        
        pattern = patterns.get(country_code)
        if pattern:
            return bool(re.match(pattern, postcode))
        
        # Default: accept any non-empty postal code for other countries
        return bool(postcode.strip())
    
    def _clean_phone_number(self, phone: str) -> str:
        """Clean and standardize phone number"""
        if not phone:
            return ""
        
        # Remove all non-digit characters except +
        cleaned = re.sub(r'[^\d+]', '', phone)
        
        if not cleaned:
            return ""
        
        # Ensure international format
        if not cleaned.startswith('+'):
            # Remove leading zeros
            cleaned = cleaned.lstrip('0')
            
            # Default to US format if length matches
            if len(cleaned) == 10:
                cleaned = f"+1{cleaned}"
            elif len(cleaned) == 11 and cleaned.startswith('1'):
                cleaned = f"+{cleaned}"
        
        return cleaned
    
    def _get_nested_value(self, data: Dict, path: str, default: Any = None) -> Any:
        """Get value from nested dictionary using dot notation"""
        if not path or not data:
            return default
        
        keys = path.split('.')
        current = data
        
        for key in keys:
            if isinstance(current, dict):
                current = current.get(key, default)
                if current is default:
                    break
            else:
                return default
        
        return current
    
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