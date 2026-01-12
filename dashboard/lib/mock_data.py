"""
Mock data for dashboard development.
Mimics the structure of mart_stripe_payments and mart_stripe_payments_daily.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta


def generate_mock_payments(days: int = 30, payments_per_day: int = 30) -> pd.DataFrame:
    """Generate mock payment data matching mart_stripe_payments schema."""
    np.random.seed(42)

    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)

    records = []
    charge_counter = 1

    for day_offset in range(days):
        current_date = start_date + timedelta(days=day_offset)
        num_payments = payments_per_day + np.random.randint(-10, 10)

        for _ in range(num_payments):
            # 94% success rate
            is_successful = np.random.random() < 0.94
            status = "succeeded" if is_successful else "failed"

            # Failure categories for failed payments
            failure_categories = [
                ("card_declined", "request_new_card"),
                ("insufficient_funds", "retry_eligible"),
                ("authentication_required", "verify_3ds"),
                ("fraud_block", "contact_support"),
                ("expired_card", "request_new_card"),
                ("processing_error", "retry_eligible"),
            ]

            if not is_successful:
                failure_cat, recovery = failure_categories[
                    np.random.choice(len(failure_categories), p=[0.35, 0.25, 0.15, 0.10, 0.10, 0.05])
                ]
            else:
                failure_cat, recovery = None, None

            # Random hour of day
            hour = np.random.randint(6, 23)
            created_at = current_date.replace(hour=hour, minute=np.random.randint(0, 59))

            records.append({
                "charge_id": f"ch_{charge_counter:08d}",
                "payment_intent_id": f"pi_{charge_counter // 2:08d}",
                "customer_id": f"cus_{np.random.randint(1000, 9999):04d}",
                "profile_id": f"prof_{np.random.randint(10000, 99999)}",
                "status": status,
                "is_successful": is_successful,
                "amount_usd": float(np.random.choice([29.99, 49.99, 99.99, 149.99], p=[0.3, 0.4, 0.2, 0.1])),
                "currency": "usd",
                "failure_code": failure_cat if not is_successful else None,
                "failure_category": failure_cat,
                "recovery_action": recovery,
                "attempt_number": np.random.choice([1, 2, 3], p=[0.85, 0.12, 0.03]),
                "created_at": created_at,
                "created_date": created_at.date(),
                "hour_of_day": hour,
                "day_of_week": created_at.strftime("%A"),
                "funnel_name": np.random.choice(["main_funnel", "promo_funnel", "referral_funnel"], p=[0.6, 0.25, 0.15]),
                "card_brand": np.random.choice(["visa", "mastercard", "amex"], p=[0.5, 0.35, 0.15]),
                "card_country": np.random.choice(["US", "GB", "DE", "CA", "AU"], p=[0.6, 0.15, 0.1, 0.1, 0.05]),
            })
            charge_counter += 1

    return pd.DataFrame(records)


def generate_mock_daily_summary(payments_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate mock payments into daily summary matching mart_stripe_payments_daily schema."""

    daily = payments_df.groupby(["created_date", "funnel_name"]).agg(
        total_attempts=("charge_id", "count"),
        successful_payments=("is_successful", "sum"),
        failed_payments=("is_successful", lambda x: (~x).sum()),
        gross_revenue_usd=("amount_usd", lambda x: x[payments_df.loc[x.index, "is_successful"]].sum()),
        failed_revenue_usd=("amount_usd", lambda x: x[~payments_df.loc[x.index, "is_successful"]].sum()),
    ).reset_index()

    daily["success_rate"] = daily["successful_payments"] / daily["total_attempts"]
    daily["date"] = daily["created_date"]

    # Add failure breakdown
    failure_counts = payments_df[~payments_df["is_successful"]].groupby(
        ["created_date", "funnel_name", "failure_category"]
    ).size().unstack(fill_value=0).reset_index()

    daily = daily.merge(failure_counts, on=["created_date", "funnel_name"], how="left")

    return daily
