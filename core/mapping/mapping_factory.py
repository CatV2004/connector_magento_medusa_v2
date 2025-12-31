from pathlib import Path
from core.mapping.product_mapping_builder import ProductMappingBuilder
from core.mapping.category_mapping_builder import CategoryMappingBuilder
from core.mapping.customer_mapping_builder import CustomerMappingBuilder
from core.mapping.address_mapping_builder import AddressMappingBuilder



class MappingFactory:
    def __init__(self, base_dir: Path):
        self.base_dir = base_dir

    def get(self, entity: str):
        if entity == "product":
            return ProductMappingBuilder(self.base_dir).build()
        elif entity == "category":
            return CategoryMappingBuilder(self.base_dir).build()
        elif entity == "customer":
            return CustomerMappingBuilder(self.base_dir).build()
        elif entity == "address":
            return AddressMappingBuilder(self.base_dir).build()
        else:
            raise ValueError(f"Unsupported entity mapping: {entity}")
