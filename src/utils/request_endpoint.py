import logging
import time
from typing import Any, Dict, Optional

import requests
from requests import Response, Session

logger = logging.getLogger(__name__)

class RequestError(Exception):
    """Erro genérico de requisição HTTP."""


class MaxRetriesExceeded(RequestError):
    """Erro quando o número máximo de tentativas é atingido."""

class RequestEndpoint:
    SUCCESS_STATUS = range(200, 300)

    def __init__(
        self,
        base_url: str = "",
        token: str = "",
        timeout: int = 30,
        max_retries: int = 5,
        backoff_factor: float = 1.5,
    ) -> None:
        self.base_url = base_url.rstrip("/") if base_url else ""
        self.token = token
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.session: Session = requests.Session()

    def _build_url(self, endpoint: str) -> str:
        if endpoint.startswith("http://") or endpoint.startswith("https://"):
            return endpoint
        return f"{self.base_url}/{endpoint.lstrip('/')}" if self.base_url else endpoint

    def _build_headers(self, headers: Optional[Dict[str, str]] = None,) -> Dict[str, str]:
        final_headers = {"Content-Type": "application/json"}

        if headers:
            final_headers.update(headers)

        if self.token and "Authorization" not in final_headers:
            final_headers["Authorization"] = f"Bearer {self.token}"

        return final_headers

    def request(
        self, 
        method: str, endpoint: str, 
        payload: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Response:

        url = self._build_url(endpoint)
        headers = self._build_headers(headers)

        attempt = 0

        while attempt <= self.max_retries:
            try:
                logger.debug("HTTP %s %s | Tentativa %s", method.upper(), url, attempt + 1,)

                response = self.session.request(
                    method=method.upper(),
                    url=url, headers=headers,
                    json=payload,
                    params=params,
                    timeout=self.timeout,
                )

                if response.status_code in self.SUCCESS_STATUS:
                    return response

                logger.warning("HTTP %s %s - Status %s", method.upper(), url, response.status_code,)

                if attempt == self.max_retries:
                    raise MaxRetriesExceeded(
                        f"Máximo de tentativas atingido. "
                        f"Status: {response.status_code}"
                    )

            except requests.RequestException as e:
                logger.error("Erro de conexão na requisição %s %s: %s", method.upper(), url, e,)

                if attempt == self.max_retries:
                    raise MaxRetriesExceeded(
                        f"Erro após {self.max_retries} tentativas"
                    ) from e

            sleep_time = self.backoff_factor * (2**attempt)
            time.sleep(sleep_time)
            attempt += 1

        raise MaxRetriesExceeded("Falha inesperada na requisição.")

    def get(self, endpoint: str, params: Optional[Dict[str, Any]] = None, **kwargs) -> Response:
        return self.request("GET", endpoint, params=params, **kwargs)

    def post(self, endpoint: str, payload: Optional[Dict[str, Any]] = None, **kwargs) -> Response:
        return self.request("POST", endpoint, payload=payload, **kwargs)

    def put(self, endpoint: str, payload: Optional[Dict[str, Any]] = None, **kwargs) -> Response:
        return self.request("PUT", endpoint, payload=payload, **kwargs)

    def delete(self, endpoint: str, payload: Optional[Dict[str, Any]] = None, **kwargs) -> Response:
        return self.request("DELETE", endpoint, payload=payload, **kwargs)

    def patch(self, endpoint: str, payload: Optional[Dict[str, Any]] = None, **kwargs) -> Response:
        return self.request("PATCH", endpoint, payload=payload, **kwargs)

    def close(self) -> None:
        self.session.close()