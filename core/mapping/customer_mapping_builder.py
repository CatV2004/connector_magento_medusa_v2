from pathlib import Path
import yaml
from typing import Dict, Any
from core.mapping.base_mapping_builder import MappingBuilder


class CustomerMappingBuilder(MappingBuilder):
    def build(self) -> Dict[str, Any]:
        return self._load("customer_mapping.yaml")