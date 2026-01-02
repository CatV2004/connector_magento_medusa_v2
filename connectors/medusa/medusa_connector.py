from config.settings import MEDUSA
from connectors.base.base_connector import BaseConnector
from connectors.medusa.medusa_auth import MedusaAuth
from utils.logger import logger
import secrets
from datetime import datetime


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

    def delete_product(self, product_id: str) -> dict:
        logger.info(f"[Medusa] Deleting product with ID: {product_id}")
        return self._request("delete", f"products/{product_id}")
    
    def delete_product_by_sku(self, sku: str) -> dict:
        logger.info(f"[Medusa] Deleting product with SKU: {sku}")
        
        result = self.get_product_by_sku(sku)
        products = result.get("products", [])
        
        if not products:
            logger.warning(f"[Medusa] Product with SKU '{sku}' not found")
            return None
        
        product_id = products[0].get("id")
        if not product_id:
            logger.error(f"[Medusa] Product found but no ID returned for SKU: {sku}")
            return None
            
        return self.delete_product(product_id)

    def get_customers(self, page: int = 1, page_size: int = 100, email: str = None) -> list[dict]:
        offset = (page - 1) * page_size
        params = {
            "limit": page_size,
            "offset": offset,
        }
        
        if email:
            params["email"] = email
            
        try:
            resp = self._request("get", "customers", params=params)
            return resp.get("customers", [])
        except Exception as e:
            logger.error(f"Failed to get customers: {e}")
            return []
        
    def get_customer(self, customer_id: str) -> dict:
        return self._request("get", f"customers/{customer_id}")
    
    def get_customer_by_email(self, email: str) -> dict:
        customers = self.get_customers(email=email, page_size=1)
        return customers[0] if customers else None

    def create_customer(self, data: dict):
        return self._request("post", "customers", json=data)
    
    def update_customer(self, customer_id: str, data: dict):
        return self._request("post", f"customers/{customer_id}", json=data)
    
    def add_address(self, customer_id: str, address_data:dict):
        return self._request("post", f"customers/{customer_id}/addresses", json=address_data)
    
    def update_address(self, customer_id: str, address_id: str, address_data: dict):
        return self._request("post", f"customers/{customer_id}/addresses/{address_id}", json=address_data)

    def update_customer(self, customer_id: str, update_data: dict) -> dict:
        logger.info(f"[Medusa] Updating customer: {customer_id}")
        
        update_data.pop('password', None)
        
        return self._request("post", f"customers/{customer_id}", json=update_data)

    def delete_customer(self, customer_id: str) -> dict:
        logger.info(f"[Medusa] Deleting customer: {customer_id}")
        
        try:
            return self._request("delete", f"customers/{customer_id}")
        except Exception as e:
            logger.error(f"[Medusa] Failed to delete customer {customer_id}: {e}")
            raise

    def send_invite(self, customer_id: str) -> dict:
        logger.info(f"[Medusa] Sending invite/reset email to customer: {customer_id}")
        
        try:
            response = self._request("post", f"customers/{customer_id}/invite", json={})
            logger.info(f"[Medusa] Invite sent successfully to customer: {customer_id}")
            return response
            
        except Exception as e:
            logger.error(f"[Medusa] Failed to send invite via dedicated endpoint: {e}")
            
            logger.info(f"[Medusa] Using fallback method for customer {customer_id}")
            
            reset_token = self._generate_reset_token()
            
            update_data = {
                "metadata": {
                    "password_reset_token": reset_token,
                    "password_reset_required": True,
                    "password_reset_sent_at": self._get_current_timestamp()
                }
            }
            
            response = self.update_customer(customer_id, update_data)
            
            logger.info(f"[Medusa] Reset token generated for customer: {customer_id}")
            return response

    def _generate_reset_token(self, length: int = 64) -> str:
        return secrets.token_urlsafe(length)
    
    def _get_current_timestamp(self) -> str:
        return datetime.now().isoformat()
    
    def create_category(self, data: dict):
        return self._request("post", "product-categories", json=data)
    
    def update_category(self, category_id: str, data: dict):
        return self._request("post", f"product-categories/{category_id}", json=data)
    
    def delete_category(self, category_id: str) -> dict:
        logger.info(f"[Medusa] Deleting category with ID: {category_id}")
        return self._request("delete", f"product-categories/{category_id}")
    
    def delete_category_by_name(self, name: str) -> dict:   
        logger.info(f"[Medusa] Deleting category with name: {name}")
        
        all_categories = self.get_categories()
        
        matching_categories = [cat for cat in all_categories if cat.get("name") == name]
        
        if not matching_categories:
            logger.warning(f"[Medusa] Category with name '{name}' not found")
            return None
        
        results = []
        for category in matching_categories:
            category_id = category.get("id")
            if category_id:
                try:
                    result = self.delete_category(category_id)
                    results.append({
                        "category_id": category_id,
                        "name": category.get("name"),
                        "result": result
                    })
                    logger.info(f"[Medusa] Successfully deleted category: {category_id} - {name}")
                except Exception as e:
                    logger.error(f"[Medusa] Failed to delete category {category_id}: {str(e)}")
                    results.append({
                        "category_id": category_id,
                        "name": category.get("name"),
                        "error": str(e)
                    })
        
        return {"deleted_categories": results}
    
    def delete_category_and_subcategories(self, category_id: str) -> dict:
        logger.info(f"[Medusa] Deleting category and subcategories for ID: {category_id}")
        
        try:
            category_info = self._request("get", f"product-categories/{category_id}")
        except Exception as e:
            logger.error(f"[Medusa] Failed to get category info for {category_id}: {str(e)}")
            return {"error": f"Failed to get category info: {str(e)}"}
        
        all_categories = self.get_categories()
        
        categories_to_delete = []
        
        def find_subcategories(parent_id, category_list):
            """Tìm đệ quy tất cả danh mục con"""
            for cat in category_list:
                if cat.get("parent_category_id") == parent_id:
                    categories_to_delete.append(cat)
                    find_subcategories(cat.get("id"), category_list)
        
        parent_category = category_info.get("product_category")
        if parent_category:
            categories_to_delete.append(parent_category)
            find_subcategories(category_id, all_categories)
        
        categories_to_delete.sort(key=lambda x: x.get("parent_category_id") is not None)
        categories_to_delete.reverse() 
        
        results = []
        for category in categories_to_delete:
            cat_id = category.get("id")
            cat_name = category.get("name")
            try:
                result = self.delete_category(cat_id)
                results.append({
                    "category_id": cat_id,
                    "name": cat_name,
                    "status": "success",
                    "result": result
                })
                logger.info(f"[Medusa] Successfully deleted category: {cat_id} - {cat_name}")
            except Exception as e:
                logger.error(f"[Medusa] Failed to delete category {cat_id}: {str(e)}")
                results.append({
                    "category_id": cat_id,
                    "name": cat_name,
                    "status": "failed",
                    "error": str(e)
                })
        
        return {"deleted_categories": results}

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

    def get_order(self, order_id: str) -> dict:
        return self._request("get", f"orders/{order_id}")
    
    def get_order_invoices(self, order_id: str) -> list[dict]:
        resp = self._request("get", f"orders/{order_id}/invoices")
        return resp.get("invoices", [])
    
    def get_order_payments(self, order_id: str) -> list[dict]:
        resp = self._request("get", f"orders/{order_id}/payments")
        return resp.get("payments", [])
    
    def search_orders(self, params: dict) -> list[dict]:
        resp = self._request("get", "orders", params=params)
        return resp.get("orders", [])
    
    def create_order(self, data: dict) -> dict:
        return self._request("post", "orders", json=data)

    def create_payment(self, data: dict) -> dict:
        return self._request("post", "payments", json=data)

    def create_invoice(self, data: dict) -> dict:   
        return self._request("post", "invoices", json=data)