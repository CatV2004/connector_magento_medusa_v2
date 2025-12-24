from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, TypeVar, Generic
import yaml
from pathlib import Path
from utils.logger import logger

T = TypeVar('T')


class BaseMapper(ABC, Generic[T]):
    """Base mapper class for all entity mappings"""
    
    def __init__(
            self, 
            mapping_config: Dict[str, Any], 
            source_connector=None, 
            target_connector=None
        ):
        self.mapping_config = mapping_config
        self.source_connector = source_connector
        self.target_connector = target_connector
        
        self.entity = self.mapping_config.get('entity', 'unknown')
        self.source_system = self.mapping_config.get('source', 'unknown')
        self.target_system = self.mapping_config.get('target', 'unknown')
        
    # def _load_mapping_config(self, mapping_file: str) -> Dict[str, Any]:
    #     try:
    #         config_path = Path(__file__).parent.parent / 'config' / 'mapping' / mapping_file
    #         with open(config_path, 'r', encoding='utf-8') as f:
    #             return yaml.safe_load(f)
    #     except Exception as e:
    #         logger.error(f"Failed to load mapping config {mapping_file}: {e}")
    #         raise
            
    @abstractmethod
    def map(self, source_data: Dict[str, Any], context: Optional[Dict] = None) -> Dict[str, Any]:
        """Map source data to target format"""
        pass
    
    def map_batch(self, source_items: List[Dict[str, Any]], context: Optional[Dict] = None) -> List[Dict[str, Any]]:
        """Map a batch of source items"""
        return [self.map(item, context) for item in source_items]
    
    def validate(self, source_data: Dict[str, Any]) -> Dict[str, List[str]]:
        """Validate source data against mapping rules"""
        errors = {
            "missing_required": [],
            "type_mismatch": [],
            "length_exceeded": []
        }
        
        validation_rules = self.mapping_config.get('validation', {})
        required_fields = validation_rules.get('required_fields', [])
        
        # Check required fields
        for field in required_fields:
            if field not in source_data or source_data[field] in (None, "", []):
                errors["missing_required"].append(field)
                
        # Check max length if specified
        max_length_rules = validation_rules.get('max_length', {})
        for field, max_len in max_length_rules.items():
            if field in source_data and source_data[field]:
                value = str(source_data[field])
                if len(value) > max_len:
                    errors["length_exceeded"].append(f"{field}: {len(value)} > {max_len}")
                    
        return errors
    
    def get_field_mapping(self, source_field: str) -> Optional[Dict[str, Any]]:
        """Get mapping configuration for a specific source field"""
        fields = self.mapping_config.get('fields', {})
        return fields.get(source_field)
    
    def get_target_field(self, source_field: str) -> Optional[str]:
        """Get target field name for a source field"""
        field_config = self.get_field_mapping(source_field)
        if field_config:
            return field_config.get('target')
        return None