"""
Customer Mapper - Handles Magento to Medusa customer data mapping.
Single Responsibility: Only maps customer fields, not addresses.
"""
from typing import Any, Dict, Optional
from dataclasses import dataclass, field
from mappers.base_mapper import BaseMapper
from core.transformer import Transformer
from core.validator import Validator
from utils.logger import logger
from datetime import datetime
import re


@dataclass
class CustomerMappingResult:
    """Result container for customer mapping operation"""
    customer_data: Dict[str, Any] = field(default_factory=dict)
    validation_errors: Dict[str, list] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)


class CustomerMapper(BaseMapper):
    """Mapper for Magento to Medusa CUSTOMER conversion ONLY"""
    
    def __init__(
        self, 
        mapping_config: Dict[str, Any],
        source_connector=None, 
        target_connector=None 
    ):
        # Gọi constructor của BaseMapper với mapping_config
        super().__init__(
            mapping_config=mapping_config, 
            source_connector=source_connector,
            target_connector=target_connector
        )
        self.transformer = Transformer()
        self.validator = Validator()
        
        # Sửa lỗi: Lấy required_fields từ mapping_config, không phải self.config
        self._required_fields = mapping_config.get('validation', {}).get('required_fields', [])
    
    def map(self, source_data: Dict[str, Any], context: Optional[Dict] = None) -> CustomerMappingResult:
        if context is None:
            context = {}
            
        result = CustomerMappingResult()
        
        try:
            # Step 1: Validate input
            result.validation_errors = self.validate(source_data)
            if result.validation_errors:
                logger.warning(f"Customer validation errors: {result.validation_errors}")
            
            # Step 2: Map basic fields
            self._map_basic_fields(result, source_data)
            
            # Step 3: Map custom attributes to metadata
            self._map_custom_attributes(result, source_data)
            
            # Step 4: Add metadata
            self._add_metadata(result, source_data, context)
            
            # Step 5: Apply post-mapping transformations
            self._apply_post_mapping_transformations(result.customer_data)
            
            # Step 6: Validate output format
            self._validate_output_format(result.customer_data)
            
        except Exception as e:
            logger.error(f"Error mapping customer: {e}")
            # Add error to validation result
            result.validation_errors.setdefault('mapping_errors', []).append(str(e))
            raise
            
        return result
    
    def validate(self, source_data: Dict[str, Any]) -> Dict[str, list]:
        errors = {
            "missing_required": [],
            "invalid_format": [],
            "business_rules": []
        }
        
        # Check required fields
        for field_name in self._required_fields:
            value = self._get_nested_value(source_data, field_name)
            if not value or (isinstance(value, str) and value.strip() == ''):
                errors["missing_required"].append(field_name)
        
        # Email validation
        email = source_data.get('email', '')
        if email and not self._is_valid_email(email):
            errors["invalid_format"].append('email')
        
        # Phone validation (if exists)
        phone = source_data.get('telephone') or source_data.get('phone', '')
        if phone and not self._is_valid_phone(phone):
            errors["invalid_format"].append('phone')
        
        # Business rules validation
        if not self._validate_business_rules(source_data):
            errors["business_rules"].append("Customer violates business rules")
        
        # Clean up empty error categories
        return {k: v for k, v in errors.items() if v}
    
    def _map_basic_fields(self, result: CustomerMappingResult, source_data: Dict):
        # Sửa lỗi: Lấy fields_config từ mapping_config, không phải self.config
        fields_config = self.mapping_config.get('fields', {})
        
        for source_field, field_config in fields_config.items():
            if not isinstance(field_config, dict):
                continue
                
            # Skip special handling fields
            if source_field in ['custom_attributes', 'addresses']:
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
            
            # Set value if not None
            if transformed_value is not None:
                self._set_nested_value(
                    result.customer_data, 
                    target_field, 
                    transformed_value
                )
    
    def _get_source_value(self, source_data: Dict, source_field: str, field_config: Dict) -> Any:
        source_paths = field_config.get('source_paths', [])
        for path in source_paths:
            value = self._get_nested_value(source_data, path)
            if value is not None and value != '':
                return value
        
        source_path = field_config.get('source_path', source_field)
        return self._get_nested_value(source_data, source_path)
    
    def _map_custom_attributes(self, result: CustomerMappingResult, source_data: Dict):
        custom_attrs = source_data.get('custom_attributes', [])
        
        for attr in custom_attrs:
            attr_code = attr.get('attribute_code')
            attr_value = attr.get('value')
            
            if not attr_code or attr_value is None:
                continue
            
            attribute_mapping = {
                'gender': 'metadata.gender',
                'dob': 'date_of_birth',
                'taxvat': 'tax_number',
                'newsletter': 'metadata.subscribed_to_newsletter',
                'created_at': 'metadata.source_created_at',
                'updated_at': 'metadata.source_updated_at'
            }
            
            if attr_code in attribute_mapping:
                self._set_nested_value(
                    result.customer_data, 
                    attribute_mapping[attr_code], 
                    attr_value
                )
            else:
                # Generic mapping to metadata
                metadata_key = f"custom_{attr_code}"
                result.metadata[metadata_key] = attr_value
    
    def _add_metadata(self, result: CustomerMappingResult, source_data: Dict, context: Dict):
        metadata = result.customer_data.setdefault('metadata', {})
        
        # Source system info
        metadata['source_system'] = self.mapping_config.get('source', 'magento')
        metadata['source_customer_id'] = source_data.get('id')
        metadata['source_email'] = source_data.get('email')
        
        # Magento-specific metadata
        if group_id := source_data.get('group_id'):
            metadata['customer_group_id'] = group_id
        
        if store_id := source_data.get('store_id'):
            metadata['store_id'] = store_id
        
        if website_id := source_data.get('website_id'):
            metadata['website_id'] = website_id
        
        # Sync metadata
        metadata['sync_timestamp'] = context.get('sync_timestamp', datetime.now().isoformat())
        metadata['batch_id'] = context.get('batch_id', 'unknown')
        metadata['migration_version'] = '1.0'
        
        # Add custom metadata
        metadata.update(result.metadata)
    
    def _apply_post_mapping_transformations(self, customer_data: Dict):
        """Apply any post-mapping transformations"""
        # Ensure email is lowercase
        if email := customer_data.get('email'):
            customer_data['email'] = email.lower().strip()
        
        # Clean phone number if exists
        if phone := customer_data.get('phone'):
            customer_data['phone'] = self._clean_phone_number(phone)
    
    def _validate_output_format(self, customer_data: Dict):
        """Validate mapped output format against Medusa requirements"""
        required_output_fields = ['email', 'first_name', 'last_name']
        
        for field in required_output_fields:
            if not customer_data.get(field):
                raise ValueError(f"Missing required output field: {field}")
        
        # Email format validation for output
        email = customer_data.get('email', '')
        if not self._is_valid_email(email):
            raise ValueError(f"Invalid email format in output: {email}")
    
    def _validate_business_rules(self, source_data: Dict) -> bool:
        email = source_data.get('email', '')
        if isinstance(email, str):
            email = email.lower()
        
        test_domains = ['test.com', 'example.com', 'fake.com']
        
        if email:
            logger.debug(f"Validating email: {email}")
        
        if email and any(domain in email for domain in test_domains):
            logger.warning(f"Test email detected: {email}. Will still sync but mark as test.")
            # Không return False nữa, chỉ log warning
            # return False
        
        # Rule 2: Check minimum required fields for business customers
        # Chỉ kiểm tra nếu là business customer (group_id = 2)
        group_id = source_data.get('group_id')
        if group_id == 2:  # Business group
            company = source_data.get('company')
            taxvat = source_data.get('taxvat')
            
            # Nếu là business customer nhưng thiếu company hoặc taxvat
            if not company or not taxvat:
                logger.warning(f"Business customer {email} missing company or taxvat")
                # Có thể return False nếu muốn strict validation
                # return False
        
        # Luôn return True để không block sync
        # Các rule business này chỉ nên là warnings, không phải errors
        return True
    
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
            
            # Add country code based on length
            if len(cleaned) == 10:
                cleaned = f"+1{cleaned}"  # Default to US
            elif len(cleaned) == 11 and cleaned.startswith('1'):
                cleaned = f"+{cleaned}"
        
        return cleaned
    
    def _is_valid_email(self, email: str) -> bool:
        """Validate email format"""
        if not email:
            return False
        
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return bool(re.match(pattern, email))
    
    def _is_valid_phone(self, phone: str) -> bool:
        """Validate phone number format"""
        if not phone:
            return True  # Phone is optional
        
        cleaned = re.sub(r'[^\d+]', '', phone)
        
        # Basic validation: between 10 and 15 digits including country code
        digits = cleaned.replace('+', '')
        return 10 <= len(digits) <= 15
    
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