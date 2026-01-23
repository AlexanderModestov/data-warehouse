"""Chargeback.io API client."""

from __future__ import annotations

import logging
from typing import Any, Generator

import requests


logger = logging.getLogger(__name__)


class ChargebackClient:
    """Client for Chargeback.io API.

    Chargeback.io aggregates alerts from multiple sources:
    - Ethoca (Mastercard alerts)
    - Verifi CDRN (Cardholder Dispute Resolution Network)
    - Verifi RDR (Rapid Dispute Resolution)

    API Base: https://api.chargeback.io/api/public/v1
    Rate Limit: 100 requests per hour per API key
    """

    DEFAULT_BASE_URL = "https://api.chargeback.io/api/public/v1"

    def __init__(
        self,
        api_key: str,
        base_url: str | None = None,
    ):
        """Initialize the Chargeback.io client.

        Args:
            api_key: API key for authentication
            base_url: Optional custom API base URL
        """
        self.api_key = api_key
        self.base_url = (base_url or self.DEFAULT_BASE_URL).rstrip("/")
        self._session = requests.Session()
        self._session.headers.update(self._get_headers())

    def _get_headers(self) -> dict:
        """Get request headers with authentication."""
        return {
            "Authorization": f"Bearer {self.api_key}",
            "X-API-Key": self.api_key,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def _request(
        self,
        method: str,
        endpoint: str,
        params: dict | None = None,
        json: dict | None = None,
    ) -> dict:
        """Make an API request.

        Args:
            method: HTTP method
            endpoint: API endpoint (without base URL)
            params: Query parameters
            json: JSON body for POST/PUT requests

        Returns:
            API response as dict

        Raises:
            requests.HTTPError: If the request fails
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"

        response = self._session.request(
            method=method,
            url=url,
            params=params,
            json=json,
            timeout=30,
        )

        response.raise_for_status()
        return response.json()

    def get_alerts(
        self,
        page: int = 1,
        page_size: int = 100,
    ) -> dict:
        """Fetch alerts from Chargeback.io API.

        Args:
            page: Page number for pagination
            page_size: Number of results per page

        Returns:
            API response with alerts data
        """
        params = {
            "page": page,
            "page_size": min(page_size, 100),
        }

        return self._request("GET", "alerts", params=params)

    def get_alert(self, alert_id: str) -> dict:
        """Get a single alert by ID.

        Args:
            alert_id: Alert identifier

        Returns:
            Alert data
        """
        return self._request("GET", f"alerts/{alert_id}")

    def iter_alerts(self) -> Generator[dict, None, None]:
        """Iterate through all alerts with automatic pagination.

        Yields:
            Individual alert records
        """
        page = 1
        page_size = 100

        while True:
            try:
                response = self.get_alerts(
                    page=page,
                    page_size=page_size,
                )
            except requests.HTTPError as e:
                logger.error(f"Failed to fetch alerts page {page}: {e}")
                raise

            # Response structure: {"count": N, "next": url|null, "previous": url|null, "results": [...]}
            alerts = response.get("results", [])

            if not alerts:
                break

            for alert in alerts:
                yield alert

            # Check if there are more pages via "next" URL
            if response.get("next") is None:
                break

            page += 1
