from typing import Any, Dict, List, Optional, Tuple
import re
from utils.logger import logger


class ValidationError(Exception):
    """Custom exception for validation errors"""
    def __init__(self, message: str, field: str = None, value: Any = None):
        self.message = message
        self.field = field
        self.value = value
        super().__init__(self.message)


class Validator:
    """Data validation utilities"""
    
    def __init__(self):
        self.errors: List[Dict] = []
        self.warnings: List[Dict] = []
        
    def reset(self):
        """Reset validation state"""
        self.errors = []
        self.warnings = []
    
    def validate_required(self, data: Dict, required_fields: List[str]) -> bool:
        """Validate that required fields are present"""
        missing = []
        for field in required_fields:
            if field not in data or data[field] in (None, "", [], {}):
                missing.append(field)
                
        if missing:
            self.errors.append({
                'type': 'missing_required',
                'fields': missing,
                'message': f"Missing required fields: {', '.join(missing)}"
            })
            return False
        return True
    
    def validate_type(self, field: str, value: Any, expected_type: str) -> bool:
        """Validate field type"""
        type_checks = {
            'string': lambda v: isinstance(v, str),
            'integer': lambda v: isinstance(v, int),
            'float': lambda v: isinstance(v, (int, float)),
            'boolean': lambda v: isinstance(v, bool),
            'array': lambda v: isinstance(v, list),
            'object': lambda v: isinstance(v, dict),
        }
        
        if expected_type not in type_checks:
            logger.warning(f"Unknown expected type: {expected_type}")
            return True
            
        if not type_checks[expected_type](value):
            self.errors.append({
                'type': 'type_mismatch',
                'field': field,
                'value': value,
                'expected': expected_type,
                'actual': type(value).__name__,
                'message': f"Field '{field}' should be {expected_type}, got {type(value).__name__}"
            })
            return False
        return True
    
    def validate_length(self, field: str, value: str, max_length: int) -> bool:
        """Validate string length"""
        if value and len(value) > max_length:
            self.errors.append({
                'type': 'length_exceeded',
                'field': field,
                'value_length': len(value),
                'max_length': max_length,
                'message': f"Field '{field}' length {len(value)} exceeds maximum {max_length}"
            })
            return False
        return True
    
    def validate_numeric_range(self, field: str, value: Any, 
                              min_val: Optional[float] = None, 
                              max_val: Optional[float] = None) -> bool:
        """Validate numeric range"""
        try:
            num = float(value)
            
            if min_val is not None and num < min_val:
                self.errors.append({
                    'type': 'below_minimum',
                    'field': field,
                    'value': value,
                    'minimum': min_val,
                    'message': f"Field '{field}' value {value} is below minimum {min_val}"
                })
                return False
                
            if max_val is not None and num > max_val:
                self.errors.append({
                    'type': 'above_maximum',
                    'field': field,
                    'value': value,
                    'maximum': max_val,
                    'message': f"Field '{field}' value {value} is above maximum {max_val}"
                })
                return False
                
        except (ValueError, TypeError):
            self.errors.append({
                'type': 'invalid_numeric',
                'field': field,
                'value': value,
                'message': f"Field '{field}' value {value} is not a valid number"
            })
            return False
            
        return True
    
    def validate_sku(self, sku: str) -> Tuple[bool, str]:
        """Validate SKU format"""
        if not sku:
            return False, "SKU cannot be empty"
            
        # Check length
        if len(sku) > 64:
            return False, f"SKU too long: {len(sku)} characters (max 64)"
            
        # Check for invalid characters
        if re.search(r'[^\w\-\.]', sku):
            return False, "SKU contains invalid characters"
            
        return True, ""
    
    def validate_price(self, price: Any) -> Tuple[bool, str]:
        """Validate price"""
        try:
            price_float = float(price)
            if price_float < 0:
                return False, "Price cannot be negative"
            if price_float > 1000000:  # $1 million limit
                return False, "Price exceeds maximum limit"
            return True, ""
        except (ValueError, TypeError):
            return False, "Invalid price format"
    
    def validate_url(self, url: str) -> bool:
        """Validate URL format"""
        if not url:
            return True
            
        url_pattern = re.compile(
            r'^(?:http|ftp)s?://'  # http:// or https://
            r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'  # domain...
            r'localhost|'  # localhost...
            r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
            r'(?::\d+)?'  # optional port
            r'(?:/?|[/?]\S+)$', re.IGNORECASE)
            
        if not re.match(url_pattern, url):
            self.warnings.append({
                'type': 'invalid_url',
                'field': 'url',
                'value': url,
                'message': f"Invalid URL format: {url}"
            })
            return False
        return True
    
    def get_validation_summary(self) -> Dict:
        """Get validation summary"""
        return {
            'has_errors': len(self.errors) > 0,
            'has_warnings': len(self.warnings) > 0,
            'error_count': len(self.errors),
            'warning_count': len(self.warnings),
            'errors': self.errors,
            'warnings': self.warnings
        }
    
    def is_valid(self) -> bool:
        """Check if validation passed without errors"""
        return len(self.errors) == 0