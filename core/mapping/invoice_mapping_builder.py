from typing import Dict, Any
from core.mapping.base_mapping_builder import MappingBuilder


class InvoiceMappingBuilder(MappingBuilder):

    def build(self) -> Dict[str, Any]:
        return self._load("invoice_mapping.yaml")