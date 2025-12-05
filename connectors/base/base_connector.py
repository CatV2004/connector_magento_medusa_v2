from abc import ABC, abstractmethod
from connectors.base.http_client import HttpClient
from typing import Optional, Dict, Any


class BaseConnector(ABC):

    def __init__(
        self, 
        base_url: str, 
        timeout: int, 
        headers: Optional[Dict[str, str]] = None, 
        verify_ssl: bool = True,
        max_retries: int = 5, 
        backoff_factor: float = 1.0,
    ): 
        self.client = HttpClient(
            base_url=base_url,
            timeout=timeout,
            default_headers=headers,
            verify_ssl=verify_ssl,
            max_retries=max_retries,
            backoff_factor=backoff_factor,
        )

    def _request(
        self,
        method: str,
        path: str,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Any] = None,
        data: Optional[Any] = None,
        timeout: Optional[int] = None,
    ) -> Any:
        if not hasattr(self.client, method.lower()):
            raise ValueError(f"Unsupported HTTP method: {method}")

        resp = getattr(self.client, method.lower())(
            path,
            headers=headers,
            params=params,
            json=json,
            data=data,
            timeout=timeout,
        )
        self._ensure_ok(resp)
        return resp.json()

    @staticmethod
    def _ensure_ok(resp):
        if not resp.ok:
            raise Exception(f"API error {resp.status_code} - {resp.text}")

    @abstractmethod
    def test_connection(self):
        pass
