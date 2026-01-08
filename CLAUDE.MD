# CLAUDE.MD - Reluvia Data Intelligence

## Project Context
Reluvia is a health/wellness project using a Web-to-App funnel.
- **Goal:** Full-funnel attribution from Meta Ads -> FunnelFox (Web) -> Stripe (Payment) -> Amplitude (App).

## Database Schema (PostgreSQL 17)
- `funnelfox_raw`: Entry point for web users.
- `tap_stripe`: Source of truth for revenue and subscription status.
- `tap_airbyte`: Product events from Amplitude.

## Key Identity Rules (The "Glue")
- **Primary Link:** `funnelfox_raw.sessions.profile_id` is the Master ID.
- **Amplitude Link:** `tap_airbyte.events.user_id` should be joined with `profile_id`.
- **Stripe Link:** `tap_stripe.charges.id` maps to `funnelfox_raw.subscriptions.psp_id`.
- **Fallback:** Use `email` or `device_id` for cross-platform matching if `user_id` is null.

## Business Logic Rules
- **Revenue:** `amount / 100.0` (Stripe stores amounts as integers in cents).
- **Successful Sale:** `tap_stripe.charges.status = 'succeeded'`.
- **Timezone:** All analysis must be done in UTC.

## Common Queries & Tasks
- **Funnel Analysis:** Start with `funnelfox_raw.sessions`, join `tap_airbyte.events` for onboarding steps, then `tap_stripe.charges` for conversion.
- **Transaction Health:** Monitor `failure_code` in `tap_stripe.charges` to identify checkout friction.

## Code Style
- Use snake_case for aliases.
- Explicitly name schemas in all SQL (e.g., `tap_stripe.charges` instead of just `charges`).