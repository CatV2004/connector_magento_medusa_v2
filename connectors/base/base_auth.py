import threading
import time
import logging
from typing import Optional

from connectors.base.http_client import HttpClient

logger = logging.getLogger(__name__)


class BaseAuth:

    login_endpoint: Optional[str] = None  # e.g. "rest/V1/integration/admin/token"
    refresh_endpoint: Optional[str] = None  # optional

    def __init__(self, base_url: str, verify_ssl: bool = False, client_kwargs: Optional[dict] = None):
        if not base_url:
            raise ValueError("Base URL missing")
        client_kwargs = client_kwargs or {}
        self.client = HttpClient(base_url=base_url, verify_ssl=verify_ssl, **client_kwargs)
        self._token: Optional[str] = None
        self._token_expires_at: Optional[float] = None  # epoch seconds
        self._lock = threading.RLock()

    def login(self) -> str:
        if not self.login_endpoint:
            raise NotImplementedError("login_endpoint must be defined or override login()")

        payload = self.build_payload()
        if payload is None:
            raise NotImplementedError("build_payload() must return JSON body")

        resp = self.client.post(self.login_endpoint, json=payload)
        if not resp.ok:
            raise Exception(f"Login failed: {resp.status_code} - {resp.text}")

        token = self.extract_token(resp)
        if not token:
            raise Exception("Login did not return token")

        with self._lock:
            self._token = token
            expires_in = self.extract_expires_in(resp)
            if expires_in:
                # subtract small safety margin (e.g., 10s)
                self._token_expires_at = time.time() + max(0, expires_in - 10)
            else:
                self._token_expires_at = None

        logger.info("Login successful, token set (expires_in=%s)", expires_in)
        return self._token

    def refresh(self) -> str:
        if self.refresh_endpoint:
            payload = self.build_refresh_payload()
            resp = self.client.post(self.refresh_endpoint, json=payload)
            if not resp.ok:
                logger.warning("Refresh failed, falling back to login: %s", resp.status_code)
                return self.login()
            token = self.extract_refresh_token(resp)
            with self._lock:
                self._token = token
                expires_in = self.extract_expires_in(resp)
                if expires_in:
                    self._token_expires_at = time.time() + max(0, expires_in - 10)
                else:
                    self._token_expires_at = None
            logger.info("Refresh successful")
            return self._token

        # fallback: re-login
        logger.debug("No refresh endpoint configured, performing full login")
        return self.login()

    def get_headers(self) -> dict:
        with self._lock:
            if not self._token:
                self.login()
                return {"Authorization": f"Bearer {self._token}", "Content-Type": "application/json"}

            if self._token_expires_at and time.time() >= self._token_expires_at:
                logger.info("Token expired or about to expire, refreshing...")
                try:
                    self.refresh()
                except Exception as e:
                    logger.warning("Refresh failed, attempting re-login: %s", e)
                    self.login()

            return {"Authorization": f"Bearer {self._token}", "Content-Type": "application/json"}

    # Must be overridden by provider
    def build_payload(self):
        raise NotImplementedError()

    # Optional override if provider gives expires_in
    def extract_expires_in(self, resp) -> Optional[int]:
        """
        Return number of seconds token is valid. Default: None (unknown).
        Override to parse provider response.
        """
        return None

    def extract_token(self, resp) -> str:
        raise NotImplementedError()

    def build_refresh_payload(self):
        return None

    def extract_refresh_token(self, resp):
        return self.extract_token(resp)