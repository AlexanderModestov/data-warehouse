"""FunnelFox tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th

from tap_funnelfox.streams import (
    FunnelsStream,
    ProductsStream,
    SessionsStream,
    SubscriptionsStream,
    ProfilesStream,
    TransactionsStream,
    SessionRepliesStream,
)


class TapFunnelFox(Tap):
    """FunnelFox tap for extracting funnel and subscription data."""

    name = "tap-funnelfox"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_key",
            th.StringType,
            required=True,
            secret=True,
            description="FunnelFox API secret key (Fox-Secret header)",
        ),
        th.Property(
            "api_base_url",
            th.StringType,
            default="https://api.funnelfox.io/public/v1",
            description="FunnelFox API base URL",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            required=False,
            description="Start date for incremental syncs (ISO 8601 format)",
        ),
        th.Property(
            "include_deleted_funnels",
            th.BooleanType,
            default=True,
            description="Include deleted funnels for FK integrity",
        ),
        th.Property(
            "page_size",
            th.IntegerType,
            default=50,
            description="Number of records per page (lower values reduce timeout risk)",
        ),
        th.Property(
            "request_timeout",
            th.IntegerType,
            default=120,
            description="API request timeout in seconds",
        ),
    ).to_dict()

    def discover_streams(self):
        """Return a list of discovered streams."""
        return [
            FunnelsStream(self),
            ProductsStream(self),
            SessionsStream(self),
            SubscriptionsStream(self),
            ProfilesStream(self),
            TransactionsStream(self),
            SessionRepliesStream(self),
        ]


if __name__ == "__main__":
    TapFunnelFox.cli()
