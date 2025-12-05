import time
import random
import logging
from typing import Optional, Dict, Any
import requests
from requests import Response, RequestException

logger = logging.getLogger(__name__)


class HttpClient:
    
    def __init__(
        self,
        base_url: str,
        timeout: int = 30,
        max_retries: int = 5,
        backoff_factor: float = 1.0,
        session: Optional[requests.Session] = None,
        verify_ssl: bool = True,
        default_headers: Optional[Dict[str, str]] = None,
    ):
        if not base_url:
            raise ValueError("base_url required")
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.verify_ssl = verify_ssl
        self._session = session or requests.Session()
        self._session.verify = verify_ssl
        self.default_headers = default_headers or {}

    def _build_url(self, endpoint: str) -> str:
        if endpoint.startswith("http://") or endpoint.startswith("https://"):
            return endpoint
        return f"{self.base_url}/{endpoint.lstrip('/')}"

    def _sleep_backoff(self, attempt: int, retry_after: Optional[float] = None):
        if retry_after is not None and retry_after > 0:
            sleep_for = retry_after
        else:
            # exponential backoff: backoff_factor * (2 ** (attempt - 1))
            base = self.backoff_factor * (2 ** (attempt - 1))
            # jitter +/- 0..base*0.1
            jitter = random.uniform(0, base * 0.1)
            sleep_for = base + jitter
        logger.debug("Sleeping for %.2f seconds before retry (attempt=%d)", sleep_for, attempt)
        time.sleep(sleep_for)

    def _request(
        self,
        method: str,
        endpoint: str,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Any] = None,
        data: Optional[Any] = None,
        timeout: Optional[int] = None,
    ) -> Response:
        url = self._build_url(endpoint)
        hdrs = {**self.default_headers, **(headers or {})}
        timeout = timeout or self.timeout

        last_exc = None
        for attempt in range(1, self.max_retries + 1):
            try:
                logger.debug("HTTP %s %s attempt=%d params=%s", method.upper(), url, attempt, params)
                resp = self._session.request(
                    method=method,
                    url=url,
                    headers=hdrs,
                    params=params,
                    json=json,
                    data=data,
                    timeout=timeout,
                )
            except RequestException as e:
                last_exc = e
                logger.warning("Network error on %s %s: %s", method.upper(), url, e)
                # treat as retryable network error
                if attempt == self.max_retries:
                    raise
                self._sleep_backoff(attempt)
                continue

            # if success
            if 200 <= resp.status_code < 300:
                return resp

            # handle rate limiting (429)
            if resp.status_code == 429:
                retry_after = None
                ra = resp.headers.get("Retry-After")
                if ra:
                    try:
                        retry_after = float(ra)
                    except ValueError:
                        # sometimes Retry-After is a HTTP-date; ignore parsing complexity here
                        retry_after = None
                logger.warning("Received 429 for %s %s. Retry-After=%s", method.upper(), url, ra)
                if attempt == self.max_retries:
                    return resp
                self._sleep_backoff(attempt, retry_after=retry_after)
                continue

            # handle server errors (5xx)
            if 500 <= resp.status_code < 600:
                logger.warning("Server error %s on %s %s", resp.status_code, method.upper(), url)
                if attempt == self.max_retries:
                    return resp
                self._sleep_backoff(attempt)
                continue

            # Client error (4xx other than 429) - do not retry by default
            logger.debug("Non-retriable response %s for %s %s", resp.status_code, method.upper(), url)
            return resp

        # exhausted retries
        if last_exc:
            raise last_exc
        raise RuntimeError("Exhausted retries but no response returned")

    def get(self, endpoint: str, **kwargs) -> Response:
        return self._request("GET", endpoint, **kwargs)

    def post(self, endpoint: str, **kwargs) -> Response:
        return self._request("POST", endpoint, **kwargs)

    def put(self, endpoint: str, **kwargs) -> Response:
        return self._request("PUT", endpoint, **kwargs)

    def delete(self, endpoint: str, **kwargs) -> Response:
        return self._request("DELETE", endpoint, **kwargs)