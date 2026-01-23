"""REST client and base stream for FunnelFox API."""

from __future__ import annotations

import time
from typing import Any, Iterable
from urllib.parse import urlparse, parse_qs

import requests
from singer_sdk.streams import RESTStream
from singer_sdk.authenticators import APIKeyAuthenticator
from singer_sdk.pagination import BaseHATEOASPaginator


class FunnelFoxPaginator(BaseHATEOASPaginator):
    """Cursor-based paginator for FunnelFox API."""

    def get_next_url(self, response: requests.Response) -> str | None:
        """Get next page URL from cursor in response."""
        data = response.json()

        # Handle endpoints that return a list directly (no pagination)
        if isinstance(data, list):
            return None

        pagination = data.get("pagination") or {}

        if not pagination.get("has_more"):
            return None

        next_cursor = pagination.get("next_cursor")
        if not next_cursor:
            return None

        # Extract current cursor from request URL to detect loops
        request_url = response.request.url
        parsed = urlparse(request_url)
        current_params = parse_qs(parsed.query)
        current_cursor = current_params.get("cursor", [None])[0]

        # Stop pagination if next_cursor equals current cursor (API bug workaround)
        if next_cursor == current_cursor:
            return None

        # Build next URL with cursor parameter
        base_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        return f"{base_url}?cursor={next_cursor}"


class FunnelFoxStream(RESTStream):
    """Base stream class for FunnelFox API."""

    records_jsonpath = "$.data[*]"

    # Retry configuration
    MAX_RETRIES = 5
    RETRY_STATUS_CODES = {408, 429, 500, 502, 503, 504, 524}

    @property
    def url_base(self) -> str:
        """Return the API base URL."""
        return self.config.get("api_base_url", "https://api.funnelfox.io/public/v1")

    @property
    def authenticator(self) -> APIKeyAuthenticator:
        """Return authenticator with Fox-Secret header."""
        return APIKeyAuthenticator.create_for_stream(
            self,
            key="Fox-Secret",
            value=self.config["api_key"],
            location="header",
        )

    @property
    def http_headers(self) -> dict:
        """Return headers for HTTP requests."""
        return {
            "Accept": "application/json",
            "User-Agent": f"{self.tap_name}/{self._tap.plugin_version}",
        }

    def get_new_paginator(self) -> FunnelFoxPaginator:
        """Return a new paginator instance."""
        return FunnelFoxPaginator()

    def get_url_params(
        self,
        context: dict | None,
        next_page_token: Any | None,
    ) -> dict[str, Any]:
        """Return URL parameters for the request."""
        params: dict[str, Any] = {
            "limit": self.config.get("page_size", 50),
        }
        # Extract cursor from paginator token if provided
        # next_page_token is a ParseResult from HATEOAS paginator
        if next_page_token:
            # Handle both ParseResult and string
            if hasattr(next_page_token, 'query'):
                query = next_page_token.query
            else:
                query = urlparse(str(next_page_token)).query
            token_params = parse_qs(query)
            if "cursor" in token_params:
                params["cursor"] = token_params["cursor"][0]
        return params

    def backoff_wait_generator(self):
        """Return generator for exponential backoff wait times."""
        def _generator():
            wait = 5
            while True:
                yield min(wait, 60)
                wait *= 2
        return _generator()

    def request_decorator(self, func):
        """Decorate request method with retry logic."""
        def wrapper(*args, **kwargs):
            retries = 0
            backoff = self.backoff_wait_generator()

            while retries < self.MAX_RETRIES:
                try:
                    response = func(*args, **kwargs)
                    response.raise_for_status()
                    return response
                except requests.exceptions.HTTPError as e:
                    if e.response is not None and e.response.status_code in self.RETRY_STATUS_CODES:
                        retries += 1
                        if retries < self.MAX_RETRIES:
                            wait_time = next(backoff)
                            self.logger.warning(
                                f"HTTP {e.response.status_code}, retrying in {wait_time}s "
                                f"(attempt {retries + 1}/{self.MAX_RETRIES})"
                            )
                            time.sleep(wait_time)
                            continue
                    raise
                except (
                    requests.exceptions.Timeout,
                    requests.exceptions.ConnectionError,
                    requests.exceptions.ChunkedEncodingError,
                ) as e:
                    retries += 1
                    if retries < self.MAX_RETRIES:
                        wait_time = next(backoff)
                        self.logger.warning(
                            f"Connection error: {type(e).__name__}, retrying in {wait_time}s "
                            f"(attempt {retries + 1}/{self.MAX_RETRIES})"
                        )
                        time.sleep(wait_time)
                        continue
                    raise

            return func(*args, **kwargs)

        return wrapper

    def _request(
        self,
        prepared_request: requests.PreparedRequest,
        context: dict | None,
    ) -> requests.Response:
        """Execute HTTP request with retry logic and rate limiting."""
        timeout = self.config.get("request_timeout", 120)

        retries = 0
        backoff = self.backoff_wait_generator()

        while retries <= self.MAX_RETRIES:
            try:
                response = self.requests_session.send(
                    prepared_request,
                    timeout=timeout,
                )

                if response.status_code in self.RETRY_STATUS_CODES:
                    retries += 1
                    if retries <= self.MAX_RETRIES:
                        wait_time = next(backoff)
                        self.logger.warning(
                            f"HTTP {response.status_code}, retrying in {wait_time}s "
                            f"(attempt {retries}/{self.MAX_RETRIES})"
                        )
                        time.sleep(wait_time)
                        # Re-prepare request for retry
                        prepared_request = self.requests_session.prepare_request(
                            requests.Request(
                                method=prepared_request.method,
                                url=prepared_request.url,
                                headers=prepared_request.headers,
                            )
                        )
                        continue

                response.raise_for_status()
                return response

            except (
                requests.exceptions.Timeout,
                requests.exceptions.ConnectionError,
                requests.exceptions.ChunkedEncodingError,
            ) as e:
                retries += 1
                if retries <= self.MAX_RETRIES:
                    wait_time = next(backoff)
                    self.logger.warning(
                        f"Connection error: {type(e).__name__}, retrying in {wait_time}s "
                        f"(attempt {retries}/{self.MAX_RETRIES})"
                    )
                    time.sleep(wait_time)
                    # Re-prepare request for retry
                    prepared_request = self.requests_session.prepare_request(
                        requests.Request(
                            method=prepared_request.method,
                            url=prepared_request.url,
                            headers=prepared_request.headers,
                        )
                    )
                    continue
                raise

        # Final attempt
        response = self.requests_session.send(prepared_request, timeout=timeout)
        response.raise_for_status()
        return response

    def post_process(
        self,
        row: dict,
        context: dict | None = None,
    ) -> dict | None:
        """Post-process a record before emitting."""
        return row

    def get_child_context(
        self,
        record: dict,
        context: dict | None,
    ) -> dict | None:
        """Return context for child streams."""
        return {"session_id": record["id"]} if self.name == "sessions" else None
