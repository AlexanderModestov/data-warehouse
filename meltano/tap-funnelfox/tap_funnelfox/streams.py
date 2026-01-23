"""Stream definitions for FunnelFox tap."""

from __future__ import annotations

from typing import Any, Iterable

from singer_sdk import typing as th

from tap_funnelfox.client import FunnelFoxStream


class FunnelsStream(FunnelFoxStream):
    """Funnels stream - funnel configurations and metadata."""

    name = "funnels"
    path = "/funnels"
    primary_keys = ["id"]
    replication_key = None  # Full table sync

    schema = th.PropertiesList(
        th.Property("id", th.StringType, required=True),
        th.Property("alias", th.StringType),
        th.Property("environment", th.StringType),
        th.Property("last_published_at", th.DateTimeType),
        th.Property("status", th.StringType),
        th.Property("tags", th.ArrayType(th.StringType)),
        th.Property("title", th.StringType),
        th.Property("type", th.StringType),
        th.Property("variation_count", th.IntegerType),
        th.Property("version", th.IntegerType),
    ).to_dict()

    def get_url_params(
        self,
        context: dict | None,
        next_page_token: str | None,
    ) -> dict[str, Any]:
        """Return URL parameters including deleted funnels filter."""
        params = super().get_url_params(context, next_page_token)
        if self.config.get("include_deleted_funnels", True):
            params["filter[deleted]"] = "true"
        return params


class ProductsStream(FunnelFoxStream):
    """Products stream - product catalog."""

    name = "products"
    path = "/products"
    primary_keys = ["id"]
    replication_key = None

    schema = th.PropertiesList(
        th.Property("id", th.StringType, required=True),
        th.Property("name", th.StringType),
        th.Property("description", th.StringType),
        th.Property("price", th.IntegerType),
        th.Property("currency", th.StringType),
        th.Property("type", th.StringType),
        th.Property("status", th.StringType),
        th.Property("created_at", th.DateTimeType),
        th.Property("updated_at", th.DateTimeType),
        # Store additional fields as JSON object
        th.Property("metadata", th.ObjectType()),
    ).to_dict()

    def post_process(self, row: dict, context: dict | None = None) -> dict | None:
        """Extract known fields and store rest in metadata."""
        known_keys = {"id", "name", "description", "price", "currency", "type", "status", "created_at", "updated_at"}
        metadata = {k: v for k, v in row.items() if k not in known_keys}
        if metadata:
            row["metadata"] = metadata
        return row


class SessionsStream(FunnelFoxStream):
    """Sessions stream - user funnel sessions."""

    name = "sessions"
    path = "/sessions"
    primary_keys = ["id"]
    replication_key = "created_at"

    schema = th.PropertiesList(
        th.Property("id", th.StringType, required=True),
        th.Property("city", th.StringType),
        th.Property("country", th.StringType),
        th.Property("created_at", th.DateTimeType, required=True),
        th.Property("funnel_id", th.StringType),
        th.Property("funnel_version", th.IntegerType),
        th.Property("ip", th.StringType),
        th.Property("origin", th.StringType),
        th.Property("postal", th.StringType),
        th.Property("profile_id", th.StringType),
        th.Property("user_agent", th.StringType),
    ).to_dict()

    def get_child_context(
        self,
        record: dict,
        context: dict | None,
    ) -> dict | None:
        """Return session_id context for child stream."""
        return {"session_id": record["id"]}


class SubscriptionsStream(FunnelFoxStream):
    """Subscriptions stream - subscription records."""

    name = "subscriptions"
    path = "/subscriptions"
    primary_keys = ["id"]
    replication_key = "created_at"

    schema = th.PropertiesList(
        th.Property("id", th.StringType, required=True),
        th.Property("billing_interval", th.StringType),
        th.Property("billing_interval_count", th.IntegerType),
        th.Property("created_at", th.DateTimeType, required=True),
        th.Property("currency", th.StringType),
        th.Property("funnel_version", th.IntegerType),
        th.Property("payment_provider", th.StringType),
        th.Property("period_ends_at", th.DateTimeType),
        th.Property("period_starts_at", th.DateTimeType),
        th.Property("price", th.IntegerType),
        th.Property("price_usd", th.IntegerType),
        th.Property("profile_id", th.StringType),
        th.Property("psp_id", th.StringType),
        th.Property("renews", th.BooleanType),
        th.Property("sandbox", th.BooleanType),
        th.Property("status", th.StringType),
        th.Property("updated_at", th.DateTimeType),
    ).to_dict()

    def post_process(self, row: dict, context: dict | None = None) -> dict | None:
        """Extract profile_id from nested profile object if present."""
        if isinstance(row.get("profile"), dict):
            row["profile_id"] = row["profile"].get("id")
            del row["profile"]
        return row


class ProfilesStream(FunnelFoxStream):
    """Profiles stream - user profiles."""

    name = "profiles"
    path = "/profiles"
    primary_keys = ["id"]
    replication_key = None

    schema = th.PropertiesList(
        th.Property("id", th.StringType, required=True),
        th.Property("email", th.StringType),
        th.Property("name", th.StringType),
        th.Property("created_at", th.DateTimeType),
        th.Property("updated_at", th.DateTimeType),
        # Store full profile data for flexibility
        th.Property("data", th.ObjectType()),
    ).to_dict()

    def post_process(self, row: dict, context: dict | None = None) -> dict | None:
        """Store full record in data field for flexibility."""
        row["data"] = dict(row)
        return row


class TransactionsStream(FunnelFoxStream):
    """Transactions stream - payment transactions."""

    name = "transactions"
    path = "/transactions"
    primary_keys = ["id"]
    replication_key = "created_at"

    schema = th.PropertiesList(
        th.Property("id", th.StringType, required=True),
        th.Property("amount", th.IntegerType),
        th.Property("currency", th.StringType),
        th.Property("status", th.StringType),
        th.Property("type", th.StringType),
        th.Property("created_at", th.DateTimeType),
        th.Property("profile_id", th.StringType),
        th.Property("subscription_id", th.StringType),
        # Store additional fields as JSON
        th.Property("data", th.ObjectType()),
    ).to_dict()

    def post_process(self, row: dict, context: dict | None = None) -> dict | None:
        """Store full record in data field."""
        row["data"] = dict(row)
        return row


class SessionRepliesStream(FunnelFoxStream):
    """Session replies stream - responses/answers within sessions."""

    name = "session_replies"
    path = "/sessions/{session_id}/replies"
    primary_keys = ["id"]
    replication_key = None
    parent_stream_type = SessionsStream

    schema = th.PropertiesList(
        th.Property("id", th.StringType, required=True),
        th.Property("session_id", th.StringType, required=True),
        th.Property("question_id", th.StringType),
        th.Property("question", th.StringType),
        th.Property("answer", th.StringType),
        th.Property("answer_type", th.StringType),
        th.Property("created_at", th.DateTimeType),
        th.Property("step", th.IntegerType),
        th.Property("data", th.ObjectType()),
    ).to_dict()

    # Override to handle 404 for sessions without replies
    ignore_parent_replication_key = True

    def get_url_params(
        self,
        context: dict | None,
        next_page_token: str | None,
    ) -> dict[str, Any]:
        """Return URL parameters."""
        # Session replies endpoint may not support pagination
        return {}

    def parse_response(self, response) -> Iterable[dict]:
        """Parse response - handle both list and wrapped formats."""
        if response.status_code == 404:
            return []

        data = response.json()

        # API may return list directly or wrapped in {"data": [...]}
        if isinstance(data, list):
            records = data
        else:
            records = data.get("data", [])

        yield from records

    def post_process(self, row: dict, context: dict | None = None) -> dict | None:
        """Add session_id from context and store full data."""
        # Skip records without an id (invalid/empty replies)
        if not row.get("id"):
            return None
        if context:
            row["session_id"] = context.get("session_id")
        row["data"] = dict(row)
        return row

    def _request(self, prepared_request, context):
        """Override to handle 404 gracefully for sessions without replies."""
        try:
            response = super()._request(prepared_request, context)
            return response
        except Exception as e:
            # Check if it's a 404 - session has no replies
            if hasattr(e, 'response') and e.response is not None:
                if e.response.status_code == 404:
                    # Return empty response-like object
                    class EmptyResponse:
                        status_code = 404
                        def json(self):
                            return []
                    return EmptyResponse()
            raise
