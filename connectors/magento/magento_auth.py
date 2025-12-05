from config.settings import MAGENTO
from connectors.base.base_auth import BaseAuth


class MagentoAuth(BaseAuth):

    login_endpoint = "integration/admin/token"

    def __init__(self):
        super().__init__(base_url=MAGENTO.BASE_URL, verify_ssl=False)
        self.token = MAGENTO.TOKEN
        self._token_expires_at = None
        
    def get_headers(self):
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }

    # def build_payload(self):
    #     if not MAGENTO.ADMIN_USERNAME or not MAGENTO.ADMIN_PASSWORD:
    #         raise ValueError("Magento admin credentials missing")

    #     return {
    #         "username": MAGENTO.ADMIN_USERNAME,
    #         "password": MAGENTO.ADMIN_PASSWORD
    #     }

    # def extract_token(self, resp):
    #     return resp.json()