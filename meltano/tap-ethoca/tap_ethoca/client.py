"""Ethoca API client with Mastercard OAuth 1.0a authentication."""

from __future__ import annotations

import hashlib
import base64
import time
import uuid
from typing import Any, Generator
from urllib.parse import urlencode, quote

import requests
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.backends import default_backend
from cryptography import x509


class EthocaClient:
    """Client for Ethoca Alerts API with Mastercard OAuth 1.0a PKI authentication."""

    SANDBOX_BASE_URL = "https://sandbox.api.mastercard.com/ethoca/v1"
    PRODUCTION_BASE_URL = "https://api.mastercard.com/ethoca/v1"

    def __init__(
        self,
        consumer_key: str,
        merchant_id: str,
        signing_key_path: str | None = None,
        signing_key_password: str | None = None,
        api_key: str | None = None,
        sandbox: bool = False,
    ):
        self.consumer_key = consumer_key
        self.merchant_id = merchant_id
        self.signing_key_path = signing_key_path
        self.signing_key_password = signing_key_password
        self.api_key = api_key
        self.sandbox = sandbox

        self.base_url = self.SANDBOX_BASE_URL if sandbox else self.PRODUCTION_BASE_URL
        self._private_key = None

        if signing_key_path:
            self._load_signing_key()

    def _load_signing_key(self) -> None:
        """Load the P12 signing key for OAuth 1.0a PKI authentication."""
        from cryptography.hazmat.primitives.serialization import pkcs12

        with open(self.signing_key_path, "rb") as f:
            p12_data = f.read()

        password = self.signing_key_password.encode() if self.signing_key_password else None
        private_key, certificate, _ = pkcs12.load_key_and_certificates(
            p12_data, password, default_backend()
        )
        self._private_key = private_key

    def _generate_oauth_signature(
        self,
        method: str,
        url: str,
        oauth_params: dict,
        body: str = "",
    ) -> str:
        """Generate OAuth 1.0a signature using RSA-SHA256."""
        # Create signature base string
        sorted_params = sorted(oauth_params.items())
        param_string = urlencode(sorted_params, quote_via=quote)

        base_string = "&".join([
            method.upper(),
            quote(url, safe=""),
            quote(param_string, safe=""),
        ])

        # Sign with RSA-SHA256
        signature = self._private_key.sign(
            base_string.encode("utf-8"),
            padding.PKCS1v15(),
            hashes.SHA256(),
        )

        return base64.b64encode(signature).decode("utf-8")

    def _get_oauth_header(self, method: str, url: str, body: str = "") -> str:
        """Build OAuth 1.0a Authorization header."""
        oauth_params = {
            "oauth_consumer_key": self.consumer_key,
            "oauth_nonce": str(uuid.uuid4()).replace("-", ""),
            "oauth_signature_method": "RSA-SHA256",
            "oauth_timestamp": str(int(time.time())),
            "oauth_version": "1.0",
        }

        # Add body hash for POST/PUT requests
        if body:
            body_hash = base64.b64encode(
                hashlib.sha256(body.encode("utf-8")).digest()
            ).decode("utf-8")
            oauth_params["oauth_body_hash"] = body_hash

        # Generate signature
        signature = self._generate_oauth_signature(method, url, oauth_params, body)
        oauth_params["oauth_signature"] = signature

        # Build header
        header_params = ", ".join([
            f'{k}="{quote(str(v), safe="")}"'
            for k, v in sorted(oauth_params.items())
        ])

        return f"OAuth {header_params}"

    def _get_headers(self, method: str, url: str, body: str = "") -> dict:
        """Get request headers with authentication."""
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        if self._private_key:
            # OAuth 1.0a PKI authentication
            headers["Authorization"] = self._get_oauth_header(method, url, body)
        elif self.api_key:
            # Simple API key authentication (alternative)
            headers["Authorization"] = f"Bearer {self.api_key}"
            headers["X-Merchant-Id"] = self.merchant_id

        return headers

    def get_alerts(
        self,
        start_date: str,
        end_date: str | None = None,
        status: str | None = None,
        page: int = 1,
        page_size: int = 100,
    ) -> dict:
        """
        Fetch alerts from Ethoca API.

        Args:
            start_date: Start date in ISO 8601 format (YYYY-MM-DD)
            end_date: Optional end date
            status: Optional filter by status (OPEN, CLOSED, etc.)
            page: Page number for pagination
            page_size: Number of results per page

        Returns:
            API response with alerts data
        """
        url = f"{self.base_url}/merchants/{self.merchant_id}/alerts"

        params = {
            "startDate": start_date,
            "page": page,
            "pageSize": page_size,
        }

        if end_date:
            params["endDate"] = end_date
        if status:
            params["status"] = status

        full_url = f"{url}?{urlencode(params)}"
        headers = self._get_headers("GET", full_url)

        response = requests.get(full_url, headers=headers, timeout=30)
        response.raise_for_status()

        return response.json()

    def iter_alerts(
        self,
        start_date: str,
        end_date: str | None = None,
        status: str | None = None,
    ) -> Generator[dict, None, None]:
        """
        Iterate through all alerts with automatic pagination.

        Yields:
            Individual alert records
        """
        page = 1
        page_size = 100

        while True:
            response = self.get_alerts(
                start_date=start_date,
                end_date=end_date,
                status=status,
                page=page,
                page_size=page_size,
            )

            alerts = response.get("alerts", [])
            if not alerts:
                break

            for alert in alerts:
                yield alert

            # Check if there are more pages
            pagination = response.get("pagination", {})
            total_pages = pagination.get("totalPages", 1)

            if page >= total_pages:
                break

            page += 1
