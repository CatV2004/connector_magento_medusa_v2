from config.settings import MAGENTO
from connectors.base.base_connector import BaseConnector
from utils.logger import logger
from connectors.magento.magento_auth import MagentoAuth
from typing import Optional


class MagentoConnector(BaseConnector):

    def __init__(self):
        self.auth = MagentoAuth()
        base_url = f"{MAGENTO.BASE_URL}"    
        timeout = MAGENTO.TIMEOUT
        headers =self.auth.get_headers()
        super().__init__(base_url, timeout=timeout, headers=headers, verify_ssl=False)
        # self.client = HttpClient(base_url=base_url, headers=headers)

    # Public API
    def test_connection(self):
        return self._request("get", "products?searchCriteria")

    def get_products(self, page=None, page_size=None):
        params = {"searchCriteria": ""}

        params.update({
            k: v
            for k, v in {
                "searchCriteria[currentPage]": page,
                "searchCriteria[pageSize]": page_size,
            }.items()
            if v is not None
        })

        return self._request("get", "products", params=params).get("items", [])

    def get_product(self, sku: str):
        return self._request("get", f"products/{sku}")
    
    def get_product_by_id(self, id: str):
        return self._request("get", f"products/{id}")

    def get_orders(self, page: Optional[int] = None, page_size: Optional[int] = None, status: Optional[str] = None):
        params = {
            "searchCriteria[currentPage]": page,
            "searchCriteria[pageSize]": page_size,
        }

        if status:
            params["searchCriteria[filter_groups][0][filters][0][field]"] = "status"
            params["searchCriteria[filter_groups][0][filters][0][value]"] = status
            params["searchCriteria[filter_groups][0][filters][0][condition_type]"] = "eq"

        resp = self._request("GET", "orders", params=params)
        return resp.get("items", [])

    def get_categories(self, page: Optional[int] = None, page_size: Optional[int] = None, level: Optional[int] = None):
        params = {
            # "searchCriteria[filterGroups][0][filters][0][field]": "parent_id",
            # "searchCriteria[filterGroups][0][filters][0][value]": parent_id,
            # "searchCriteria[filterGroups][0][filters][0][condition_type]": "eq",
            "searchCriteria[currentPage]": page,
            "searchCriteria[pageSize]": page_size,
        }

        if level is not None:
            params.update({
                "searchCriteria[filterGroups][1][filters][0][field]": "level",
                "searchCriteria[filterGroups][1][filters][0][value]": level,
                # "searchCriteria[filterGroups][1][filters][0][condition_type]": "eq",
            })
        logger.info(f"Fetching categories with params: {params}")

        return self._request(method="get", path="categories/list", params=params).get("items", [])
    
    def get_categories_by_parent(self, parent_id: int):
        params = {
            "searchCriteria[filterGroups][0][filters][0][field]": "parent_id",
            "searchCriteria[filterGroups][0][filters][0][value]": parent_id,
            "searchCriteria[filterGroups][0][filters][0][condition_type]": "eq",
        }
        return self._request(method="get", path="/categories/list", params=params).get("items", [])

    
    def get_all_categories_tree(self) -> list[dict]:

        try:
            response = self._request(
                method="get", 
                path="categories/list?searchCriteria"
            )
            items = response.get("items", [])
            logger.info(f"[Magento] Retrieved {len(items)} categories")

            if not items:
                return []

            node_map = {}
            for item in items:
                node_map[item["id"]] = {
                    "id": item["id"],
                    "name": item.get("name", "Unnamed Category").strip(),
                    "children": []
                }

            tree = []
            for item in items:
                node = node_map[item["id"]]
                parent_id = item.get("parent_id")
                if parent_id and parent_id in node_map and parent_id != item["id"]:
                    node_map[parent_id]["children"].append(node)
                else:
                    tree.append(node)

            logger.info(f"[Magento] Categories tree built with {len(tree)} root nodes")
            return tree

        except Exception as e:
            logger.error(f"[Magento] Failed to fetch categories tree: {e}")
            return []

    def create_product(self, data: dict):
        return self._request("post", "products", json=data)

    def update_product(self, sku: str, data: dict):
        return self._request("put", f"products/{sku}", json=data)

    def get_all_products_map(self, page, page_size) -> dict:
        products = self.get_products(page=page, page_size=page_size)
        return {p["id"]: p for p in products}
    
    def get_attribute_options(self, attribute_code: str) -> dict:
        return self._request("get", f"products/attributes/{attribute_code}").get("options", {})
    
    def get_customers(self, page: Optional[int] = None, page_size: Optional[int] = None):
        params ={
            "searchCriteria[currentPage]": page,
            "searchCriteria[pageSize]": page_size,
        }
        
        return self._request("get", "customers/search", params=params).get("items", []) 
    
    def get_customer(self, customer_id:int):
        return self._request("get", f"customers/{customer_id}")