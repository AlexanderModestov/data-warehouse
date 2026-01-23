"""Chargeback.io tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th

from tap_chargeback.streams import AlertsStream


class TapChargeback(Tap):
    """Chargeback.io tap for extracting alerts from Ethoca, Verifi CDRN, and RDR."""

    name = "tap-chargeback"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_key",
            th.StringType,
            required=True,
            secret=True,
            description="Chargeback.io API key",
        ),
        th.Property(
            "api_base_url",
            th.StringType,
            default="https://api.chargeback.io/api/public/v1",
            description="Chargeback.io API base URL",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            required=True,
            description="Start date for fetching alerts (ISO 8601 format)",
        ),
    ).to_dict()

    def discover_streams(self):
        """Return a list of discovered streams."""
        return [AlertsStream(self)]


if __name__ == "__main__":
    TapChargeback.cli()
