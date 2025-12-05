from config.settings import MEDUSA
from connectors.base.base_auth import BaseAuth


class MedusaAuth(BaseAuth):
    login_endpoint = "auth/user/emailpass"
    # If Medusa had a refresh endpoint, set refresh_endpoint = "auth/refresh"

    def __init__(self):
        super().__init__(base_url=MEDUSA.BASE_URL)

    def build_payload(self):
        if not MEDUSA.ADMIN_EMAIL or not MEDUSA.ADMIN_PASSWORD:
            raise ValueError("Medusa admin credentials missing")
        return {"email": MEDUSA.ADMIN_EMAIL, "password": MEDUSA.ADMIN_PASSWORD}

    def extract_token(self, resp):
        body = resp.json()
        return body.get("token")

    def extract_expires_in(self, resp):
        body = resp.json()
        return body.get("expires_in")