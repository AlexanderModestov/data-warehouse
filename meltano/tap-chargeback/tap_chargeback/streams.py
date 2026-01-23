"""Stream definitions for Chargeback.io tap."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Iterable

from singer_sdk import typing as th
from singer_sdk.streams import Stream

from tap_chargeback.client import ChargebackClient


class AlertsStream(Stream):
    """Stream for Chargeback.io Alerts (Ethoca, CDRN, RDR)."""

    name = "chargeback_alerts"
    primary_keys = ["external_id"]
    replication_key = "created_at"
    replication_method = "INCREMENTAL"

    schema = th.PropertiesList(
        # Alert identifiers
        th.Property("external_id", th.StringType, description="Unique external identifier (UUID)"),
        th.Property("alert_id", th.StringType, description="Alert ID from source network"),
        th.Property("task_id", th.StringType, description="Internal task identifier"),

        # Alert classification
        th.Property("alert_type", th.StringType, description="Type: fraud, dispute"),
        th.Property("alert_status", th.StringType, description="Status: Auto-refunded, Pending, etc."),
        th.Property("alert_service", th.StringType, description="Service: ethoca, rdr, cdrn"),
        th.Property("service_status", th.StringType, description="Service processing status"),
        th.Property("alert_age", th.IntegerType, description="Age of alert in hours"),

        # Transaction details
        th.Property("arn", th.StringType, description="Acquirer Reference Number - links to Stripe"),
        th.Property("auth_code", th.StringType, description="Authorization code"),
        th.Property("transaction_id", th.StringType, description="Transaction ID if available"),
        th.Property("amount", th.StringType, description="Original transaction amount"),
        th.Property("converted_amount", th.StringType, description="Amount in converted currency"),
        th.Property("currency", th.StringType, description="Original currency code"),
        th.Property("converted_currency", th.StringType, description="Converted currency code"),
        th.Property("transaction_timestamp", th.StringType, description="Original transaction timestamp"),
        th.Property("transaction_type", th.StringType, description="Transaction type (keyed, etc.)"),
        th.Property("mcc", th.StringType, description="Merchant Category Code"),

        # Card details
        th.Property("card_last4", th.StringType, description="Last 4 digits of card"),
        th.Property("card_bin", th.StringType, description="Card BIN (first 6 digits)"),
        th.Property("brand", th.StringType, description="Card brand (Visa, Mastercard)"),
        th.Property("payment_type", th.StringType, description="Payment type (VISA, etc.)"),
        th.Property("is_3d_secure", th.StringType, description="3D Secure status (yes/no)"),

        # Alert reason
        th.Property("reason_code", th.StringType, description="Network reason code"),
        th.Property("alert_reason", th.StringType, description="Human-readable alert reason"),
        th.Property("source", th.StringType, description="Alert source (MASTERCARD, vmpi, Issuer)"),
        th.Property("initiated_by", th.StringType, description="Who initiated the alert"),

        # Issuer information
        th.Property("issuer", th.StringType, description="Card issuer name"),
        th.Property("bank_name", th.StringType, description="Issuing bank name"),
        th.Property("country_code", th.StringType, description="Country code (US, etc.)"),
        th.Property("country", th.StringType, description="Country name"),

        # Merchant information
        th.Property("merchant_descriptor", th.StringType, description="Billing descriptor"),
        th.Property("merchant_member_name", th.StringType, description="Merchant member name"),
        th.Property("payment_processor", th.StringType, description="Payment processor (stripe)"),
        th.Property("business_account_name", th.StringType, description="Business account name"),
        th.Property("business_account_url", th.StringType, description="Business account URL"),

        # Customer information
        th.Property("subscribe_customer", th.StringType, description="Customer email"),
        th.Property("customer_full_name", th.StringType, description="Customer full name"),
        th.Property("unsubscribe_status", th.StringType, description="Unsubscribe status"),
        th.Property("unsubscribe_at", th.StringType, description="Unsubscribe timestamp"),

        # Timestamps
        th.Property("alert_timestamp", th.StringType, description="When alert was received"),
        th.Property("created_at", th.StringType, description="Record creation timestamp"),
        th.Property("updated_at", th.StringType, description="Record last update timestamp"),
        th.Property("auto_refunded_at", th.StringType, description="Auto-refund timestamp"),

        # Status and billing
        th.Property("paid_status", th.StringType, description="Payment status (Paid, etc.)"),
        th.Property("alert_cost", th.IntegerType, description="Alert cost in cents"),
        th.Property("is_duplicated", th.BooleanType, description="Whether alert is a duplicate"),
        th.Property("is_manual", th.BooleanType, description="Whether manually created"),
        th.Property("is_demo", th.BooleanType, description="Whether demo/test alert"),
        th.Property("error_message", th.StringType, description="Error message if any"),

        # Additional fields
        th.Property("descriptor_contact", th.StringType, description="Descriptor contact info"),
        th.Property("network_time", th.StringType, description="Network processing time"),
        th.Property("procession_status", th.StringType, description="Processing status"),
        th.Property("ac_exp_num", th.StringType, description="Account expiration number"),
    ).to_dict()

    def __init__(self, tap, **kwargs):
        super().__init__(tap, **kwargs)
        self._client = None

    @property
    def client(self) -> ChargebackClient:
        """Get or create Chargeback.io API client."""
        if self._client is None:
            self._client = ChargebackClient(
                api_key=self.config["api_key"],
                base_url=self.config.get("api_base_url"),
            )
        return self._client

    def get_records(self, context: dict | None) -> Iterable[dict]:
        """Get alert records from Chargeback.io API.

        Yields:
            Alert records matching schema
        """
        # Determine start date from state or config for filtering
        start_date = self.get_starting_replication_key_value(context)

        if start_date is None:
            start_date = self.config["start_date"]

        if isinstance(start_date, datetime):
            start_date_dt = start_date
        else:
            start_date_dt = datetime.fromisoformat(str(start_date).replace("Z", "+00:00"))

        self.logger.info(f"Fetching alerts created after {start_date_dt}")

        for raw_alert in self.client.iter_alerts():
            # Filter by start_date since API doesn't support date filtering
            created_at = raw_alert.get("created_at")
            if created_at:
                try:
                    alert_dt = datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S")
                    if alert_dt < start_date_dt.replace(tzinfo=None):
                        continue
                except (ValueError, TypeError):
                    pass

            # Skip demo alerts unless explicitly wanted
            if raw_alert.get("is_demo", False):
                continue

            yield self._normalize_alert(raw_alert)

    def _normalize_alert(self, raw: dict) -> dict:
        """Pass through API response fields matching our schema.

        Args:
            raw: Raw alert from API

        Returns:
            Alert record matching schema
        """
        return {
            # Identifiers
            "external_id": raw.get("external_id"),
            "alert_id": raw.get("alert_id"),
            "task_id": raw.get("task_id"),

            # Classification
            "alert_type": raw.get("alert_type"),
            "alert_status": raw.get("alert_status"),
            "alert_service": raw.get("alert_service"),
            "service_status": raw.get("service_status"),
            "alert_age": raw.get("alert_age"),

            # Transaction
            "arn": raw.get("arn"),
            "auth_code": raw.get("auth_code"),
            "transaction_id": raw.get("transaction_id"),
            "amount": raw.get("amount"),
            "converted_amount": raw.get("converted_amount"),
            "currency": raw.get("currency"),
            "converted_currency": raw.get("converted_currency"),
            "transaction_timestamp": raw.get("transaction_timestamp"),
            "transaction_type": raw.get("transaction_type"),
            "mcc": raw.get("mcc"),

            # Card
            "card_last4": raw.get("card_last4"),
            "card_bin": raw.get("card_bin"),
            "brand": raw.get("brand"),
            "payment_type": raw.get("payment_type"),
            "is_3d_secure": raw.get("is_3d_secure"),

            # Reason
            "reason_code": raw.get("reason_code"),
            "alert_reason": raw.get("alert_reason"),
            "source": raw.get("source"),
            "initiated_by": raw.get("initiated_by"),

            # Issuer
            "issuer": raw.get("issuer"),
            "bank_name": raw.get("bank_name"),
            "country_code": raw.get("country_code"),
            "country": raw.get("country"),

            # Merchant
            "merchant_descriptor": raw.get("merchant_descriptor"),
            "merchant_member_name": raw.get("merchant_member_name"),
            "payment_processor": raw.get("payment_processor"),
            "business_account_name": raw.get("business_account_name"),
            "business_account_url": raw.get("business_account_url"),

            # Customer
            "subscribe_customer": raw.get("subscribe_customer"),
            "customer_full_name": raw.get("customer_full_name"),
            "unsubscribe_status": raw.get("unsubscribe_status"),
            "unsubscribe_at": raw.get("unsubscribe_at"),

            # Timestamps
            "alert_timestamp": raw.get("alert_timestamp"),
            "created_at": raw.get("created_at"),
            "updated_at": raw.get("updated_at"),
            "auto_refunded_at": raw.get("auto_refunded_at"),

            # Status
            "paid_status": raw.get("paid_status"),
            "alert_cost": raw.get("alert_cost"),
            "is_duplicated": raw.get("is_duplicated"),
            "is_manual": raw.get("is_manual"),
            "is_demo": raw.get("is_demo"),
            "error_message": raw.get("error_message"),

            # Additional
            "descriptor_contact": raw.get("descriptor_contact"),
            "network_time": raw.get("network_time"),
            "procession_status": raw.get("procession_status"),
            "ac_exp_num": raw.get("ac_exp_num"),
        }
