from typing import Any, Dict, Optional, List
from mappers.base_mapper import BaseMapper
from core.transformer import Transformer
from core.validator import Validator
from utils.logger import logger
import re


class CustomerMapper(BaseMapper):
    """Mapper for Magento to Medusa customer conversion"""
    
    def __init__(self, source_connector=None, target_connector=None):
        super().__init__('customer_mapping.yaml', source_connector, target_connector)
        self.transformer = Transformer()
        self.validator = Validator()
        
    def map(self, source_data: Dict[str, Any], context: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Map Magento customer to Medusa format
        
        Args:
            source_data: Magento customer data
            context: Additional context
            
        Returns:
            Dict: Medusa formatted customer
        """
        if context is None:
            context = {}
            
        # Initialize result
        result = {
            "metadata": {},
            "billing_address": {},
            "shipping_address": {}
        }
        
        # Map basic fields
        self._map_basic_fields(result, source_data)
        
        # Map addresses
        self._map_addresses(result, source_data)
        
        # Map custom attributes
        self._map_custom_attributes(result, source_data)
        
        # Add metadata
        result = self._add_metadata(result, source_data, context)
        
        return result
    
    def _map_basic_fields(self, result: Dict, source_data: Dict):
        """Map basic customer fields"""
        # Email
        email = source_data.get('email', '')
        if email:
            result['email'] = email.lower().strip()
            result['has_account'] = True
        
        # Name
        firstname = source_data.get('firstname', '')
        lastname = source_data.get('lastname', '')
        
        if firstname or lastname:
            result['first_name'] = firstname.strip()
            result['last_name'] = lastname.strip()
            result['full_name'] = f"{firstname} {lastname}".strip()
        
        # Company
        company = source_data.get('company', '')
        if company:
            result['company_name'] = company.strip()
        
        # Phone
        telephone = source_data.get('telephone', '') or source_data.get('phone', '')
        if telephone:
            result['phone'] = self._clean_phone(telephone)
        
        # Dates
        created_at = source_data.get('created_at')
        if created_at:
            result['created_at'] = created_at
        
        updated_at = source_data.get('updated_at')
        if updated_at:
            result['updated_at'] = updated_at
        
        # Status
        is_active = source_data.get('is_active', True)
        result['has_account'] = bool(is_active)
    
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
    
    def _map_addresses(self, result: Dict, source_data: Dict):
        """Map customer addresses"""
        addresses = source_data.get('addresses', [])
        
        for address in addresses:
            address_type = 'shipping' if address.get('is_default_shipping') else 'billing'
            
            address_data = self._map_single_address(address)
            
            if address_type == 'shipping':
                result['shipping_address'] = address_data
            elif address_type == 'billing':
                result['billing_address'] = address_data
            
            # If no default specified, use first address
            if not result.get('shipping_address') and address_type != 'billing':
                result['shipping_address'] = address_data
            if not result.get('billing_address') and address_type != 'shipping':
                result['billing_address'] = address_data
    
    def _map_single_address(self, address: Dict) -> Dict:
        """Map single address"""
        return {
            'first_name': address.get('firstname', ''),
            'last_name': address.get('lastname', ''),
            'address_1': address.get('street', [''])[0],
            'address_2': address.get('street', ['', ''])[1] if len(address.get('street', [])) > 1 else '',
            'city': address.get('city', ''),
            'postal_code': address.get('postcode', ''),
            'province': address.get('region', {}).get('region', ''),
            'country_code': self._map_country_code(address.get('country_id', '')),
            'phone': address.get('telephone', ''),
            'company': address.get('company', '')
        }
    
    def _map_country_code(self, magento_country_id: str) -> str:
        """Map Magento country ID to ISO country code"""
        country_map = {
            'US': 'us',
            'GB': 'gb',
            'CA': 'ca',
            'AU': 'au',
            'DE': 'de',
            'FR': 'fr',
            'IT': 'it',
            'ES': 'es',
            'JP': 'jp',
            'CN': 'cn',
            'IN': 'in',
            'BR': 'br',
            'MX': 'mx',
        }
        
        return country_map.get(magento_country_id.upper(), magento_country_id.lower())
    
    def _map_custom_attributes(self, result: Dict, source_data: Dict):
        """Map custom attributes to metadata"""
        custom_attrs = source_data.get('custom_attributes', [])
        
        for attr in custom_attrs:
            attr_code = attr.get('attribute_code')
            attr_value = attr.get('value')
            
            if attr_code and attr_value is not None:
                metadata_key = f"magento_{attr_code}"
                result['metadata'][metadata_key] = attr_value
    
    def _add_metadata(self, result: Dict, source_data: Dict, context: Dict) -> Dict:
        """Add additional metadata"""
        result['metadata']['source_system'] = self.source_system
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
        result['metadata']['sync_timestamp'] = context.get('sync_timestamp')
        
        return result
    
    def validate_customer(self, customer_data: Dict) -> Dict[str, List[str]]:
        """Validate customer data"""
        errors = {
            "missing_required": [],
            "invalid_format": []
        }
        
        # Check required fields
        required_fields = ['email', 'firstname', 'lastname']
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