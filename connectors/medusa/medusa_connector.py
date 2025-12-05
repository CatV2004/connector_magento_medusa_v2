from config.settings import MEDUSA
from connectors.base.base_connector import BaseConnector
from connectors.medusa.medusa_auth import MedusaAuth
from utils.logger import logger


class MedusaConnector(BaseConnector):

    def __init__(self):
        self.auth = MedusaAuth()
        base_url = f"{MEDUSA.BASE_URL}/admin" 
        timeout = MEDUSA.TIMEOUT
        headers = self.auth.get_headers()
        super().__init__(base_url, timeout=timeout, headers=headers)
        # self.client = HttpClient(base_url=base_url, headers=headers)

    def test_connection(self):
        return self._request("get", "products")
    
    def get_products(self, page: int | None = 1, page_size: int = 100) -> list[dict]:
        offset = 0 if page in (None, 1) else (page - 1) * page_size
        params = {"limit": page_size, "offset": offset}
        resp = self._request("get", "products", params=params)
        return resp.get("products", [])
      
    def get_product_by_sku(self, sku: str):
        return self._request("get", f"products?variants[sku]={sku}")
    
    def get_product_by_id(self, id: str):
        return self._request("get", f"products/{id}")

    def create_product(self, data: dict):
        logger.info(f"[Medusa] Creating product with data: {data}")
        return self._request("post", "products", json=data)
    
    def update_product(self, product_id: str, data: dict):
        return self._request("post", f"products/{product_id}", json=data)

    def create_customer(self, data: dict):
        return self._request("post", "customers", json=data)
    
    def create_category(self, data: dict):
        return self._request("post", "product-categories", json=data)
    
    def update_category(self, category_id: str, data: dict):
        return self._request("post", f"product-categories/{category_id}", json=data)

    def get_categories(self, limit: int = 100) -> list[dict]:
        all_categories = []
        offset = 0

        while True:
            params = {"limit": limit, "offset": offset}
            resp = self._request("get", "product-categories", params=params)
            categories = resp.get("product_categories", [])
            all_categories.extend(categories)

            if len(categories) < limit:
                break 
            offset += limit

        return all_categories