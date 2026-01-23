"""Stream definitions for Ethoca tap."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Iterable

from singer_sdk import typing as th
from singer_sdk.streams import Stream

from tap_ethoca.client import EthocaClient


class AlertsStream(Stream):
    """Stream for Ethoca Alerts."""

    name = "alerts"
    primary_keys = ["alert_id"]
    replication_key = "created_at"
    replication_method = "INCREMENTAL"

    schema = th.PropertiesList(
        # Alert identifiers
        th.Property("alert_id", th.StringType, description="Unique alert identifier"),
        th.Property("merchant_id", th.StringType, description="Merchant identifier"),
        th.Property("alert_type", th.StringType, description="Type of alert (FRAUD, DISPUTE)"),

        # Transaction details
        th.Property("transaction_id", th.StringType, description="Original transaction ID"),
        th.Property("arn", th.StringType, description="Acquirer Reference Number"),
        th.Property("transaction_amount", th.NumberType, description="Transaction amount"),
        th.Property("transaction_currency", th.StringType, description="Transaction currency code"),
        th.Property("transaction_date", th.DateTimeType, description="Original transaction date"),

        # Card details
        th.Property("card_brand", th.StringType, description="Card brand (VISA, MASTERCARD, etc.)"),
        th.Property("card_last_four", th.StringType, description="Last 4 digits of card"),
        th.Property("card_country", th.StringType, description="Card issuing country"),

        # Alert details
        th.Property("reason_code", th.StringType, description="Reason code for alert"),
        th.Property("reason_description", th.StringType, description="Human-readable reason"),
        th.Property("fraud_type", th.StringType, description="Type of fraud (if fraud alert)"),
        th.Property("status", th.StringType, description="Alert status (OPEN, CLOSED, RESOLVED)"),
        th.Property("resolution", th.StringType, description="Resolution type if closed"),
        th.Property("refund_status", th.StringType, description="Refund status"),
        th.Property("refund_amount", th.NumberType, description="Refund amount if refunded"),

        # Timestamps
        th.Property("created_at", th.DateTimeType, description="Alert creation timestamp"),
        th.Property("updated_at", th.DateTimeType, description="Alert last update timestamp"),
        th.Property("resolved_at", th.DateTimeType, description="Alert resolution timestamp"),
        th.Property("deadline", th.DateTimeType, description="Response deadline"),

        # Issuer information
        th.Property("issuer_name", th.StringType, description="Card issuer name"),
        th.Property("issuer_country", th.StringType, description="Issuer country"),

        # Additional metadata
        th.Property("descriptor", th.StringType, description="Merchant descriptor"),
        th.Property("comments", th.StringType, description="Comments or notes"),
        th.Property("raw_data", th.ObjectType(), description="Full raw API response"),
    ).to_dict()

    def __init__(self, tap, **kwargs):
        super().__init__(tap, **kwargs)
        self._client = None

    @property
    def client(self) -> EthocaClient:
        """Get or create Ethoca API client."""
        if self._client is None:
            self._client = EthocaClient(
                consumer_key=self.config["consumer_key"],
                merchant_id=self.config["merchant_id"],
                signing_key_path=self.config.get("signing_key_path"),
                signing_key_password=self.config.get("signing_key_password"),
                api_key=self.config.get("api_key"),
                sandbox=self.config.get("sandbox", False),
            )
        return self._client

    def get_records(self, context: dict | None) -> Iterable[dict]:
        """
        Get alert records from Ethoca API.

        Yields:
            Normalized alert records
        """
        # Determine start date from state or config
        start_date = self.get_starting_replication_key_value(context)

        if start_date is None:
            start_date = self.config["start_date"]

        if isinstance(start_date, datetime):
            start_date_str = start_date.strftime("%Y-%m-%d")
        else:
            start_date_str = start_date[:10]  # Extract date portion

        # End date is today
        end_date_str = datetime.utcnow().strftime("%Y-%m-%d")

        self.logger.info(f"Fetching alerts from {start_date_str} to {end_date_str}")

        for raw_alert in self.client.iter_alerts(
            start_date=start_date_str,
            end_date=end_date_str,
        ):
            yield self._normalize_alert(raw_alert)

    def _normalize_alert(self, raw: dict) -> dict:
        """
        Normalize raw API response to schema.

        Args:
            raw: Raw alert from API

        Returns:
            Normalized alert record
        """
        # Map API response fields to our schema
        # Field names may vary based on actual Ethoca API response structure
        return {
            "alert_id": raw.get("alertId") or raw.get("id"),
            "merchant_id": raw.get("merchantId") or self.config["merchant_id"],
            "alert_type": raw.get("alertType") or raw.get("type"),

            # Transaction details
            "transaction_id": raw.get("transactionId") or raw.get("transactionReference"),
            "arn": raw.get("arn") or raw.get("acquirerReferenceNumber"),
            "transaction_amount": self._parse_amount(raw.get("transactionAmount") or raw.get("amount")),
            "transaction_currency": raw.get("transactionCurrency") or raw.get("currency"),
            "transaction_date": raw.get("transactionDate") or raw.get("transactionTimestamp"),

            # Card details
            "card_brand": raw.get("cardBrand") or raw.get("cardNetwork"),
            "card_last_four": raw.get("cardLastFour") or raw.get("last4"),
            "card_country": raw.get("cardCountry") or raw.get("cardIssuingCountry"),

            # Alert details
            "reason_code": raw.get("reasonCode"),
            "reason_description": raw.get("reasonDescription") or raw.get("reason"),
            "fraud_type": raw.get("fraudType"),
            "status": raw.get("status"),
            "resolution": raw.get("resolution") or raw.get("outcome"),
            "refund_status": raw.get("refundStatus"),
            "refund_amount": self._parse_amount(raw.get("refundAmount")),

            # Timestamps
            "created_at": raw.get("createdAt") or raw.get("alertDate") or raw.get("created"),
            "updated_at": raw.get("updatedAt") or raw.get("lastModified"),
            "resolved_at": raw.get("resolvedAt") or raw.get("closedDate"),
            "deadline": raw.get("deadline") or raw.get("responseDeadline"),

            # Issuer information
            "issuer_name": raw.get("issuerName") or raw.get("issuingBank"),
            "issuer_country": raw.get("issuerCountry"),

            # Additional metadata
            "descriptor": raw.get("descriptor") or raw.get("merchantDescriptor"),
            "comments": raw.get("comments") or raw.get("notes"),

            # Keep raw data for reference
            "raw_data": raw,
        }

    def _parse_amount(self, value: Any) -> float | None:
        """Parse amount value to float."""
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        try:
            # Handle string amounts, possibly with currency symbols
            cleaned = str(value).replace(",", "").replace("$", "").replace(" ", "")
            return float(cleaned)
        except (ValueError, TypeError):
            return None
