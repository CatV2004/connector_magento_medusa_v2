from typing import Dict, Any
from core.mapping.base_mapping_builder import MappingBuilder


class AddressMappingBuilder(MappingBuilder):

    def build(self) -> Dict[str, Any]:
        return self._load("address_mapping.yaml")