"""Ethoca tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th

from tap_ethoca.streams import AlertsStream


class TapEthoca(Tap):
    """Ethoca tap for extracting alerts data from Mastercard Ethoca API."""

    name = "tap-ethoca"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "consumer_key",
            th.StringType,
            required=True,
            description="Mastercard API consumer key",
        ),
        th.Property(
            "signing_key_path",
            th.StringType,
            required=False,
            description="Path to P12 signing key file (for OAuth 1.0a PKI)",
        ),
        th.Property(
            "signing_key_password",
            th.StringType,
            required=False,
            secret=True,
            description="Password for the P12 signing key",
        ),
        th.Property(
            "api_key",
            th.StringType,
            required=False,
            secret=True,
            description="API key (alternative to OAuth 1.0a authentication)",
        ),
        th.Property(
            "merchant_id",
            th.StringType,
            required=True,
            description="Ethoca merchant identifier",
        ),
        th.Property(
            "api_base_url",
            th.StringType,
            default="https://api.mastercard.com/ethoca/v1",
            description="Ethoca API base URL",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            required=True,
            description="Start date for fetching alerts (ISO 8601 format)",
        ),
        th.Property(
            "sandbox",
            th.BooleanType,
            default=False,
            description="Use sandbox environment",
        ),
    ).to_dict()

    def discover_streams(self):
        """Return a list of discovered streams."""
        return [AlertsStream(self)]


if __name__ == "__main__":
    TapEthoca.cli()
