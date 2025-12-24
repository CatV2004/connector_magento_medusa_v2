from typing import Dict, Any
from core.mapping.base_mapping_builder import MappingBuilder


class ProductMappingBuilder(MappingBuilder):
    def build(self) -> Dict[str, Any]:
        base = self._load("product_mapping.yaml")
        defaults = self._load("defaults/medusa_product.yaml")

        return self._merge(base, defaults)

    @staticmethod
    def deep_merge(a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Any]:
        for k, v in b.items():
            if (
                k in a
                and isinstance(a[k], dict)
                and isinstance(v, dict)
            ):
                MappingBuilder.deep_merge(a[k], v)
            else:
                a[k] = v
        return a

    def _merge(self, base: Dict[str, Any], defaults: Dict[str, Any]) -> Dict[str, Any]:
        return self.deep_merge(base, defaults)
