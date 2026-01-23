--
-- PostgreSQL database dump
--

-- Dumped from database version 17.6
-- Dumped by pg_dump version 17.0

-- Started on 2026-01-19 16:27:02

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- TOC entry 233 (class 2615 OID 28278854)
-- Name: raw_stripe; Type: SCHEMA; Schema: -; Owner: uefdt0t8idi0oj
--

CREATE SCHEMA raw_stripe;


ALTER SCHEMA raw_stripe OWNER TO uefdt0t8idi0oj;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- TOC entry 614 (class 1259 OID 28278942)
-- Name: balance_transactions; Type: TABLE; Schema: raw_stripe; Owner: uefdt0t8idi0oj
--

CREATE TABLE raw_stripe.balance_transactions (
    fee bigint,
    currency text,
    source text,
    fee_details jsonb,
    available_on bigint,
    status text,
    description text,
    net bigint,
    exchange_rate numeric,
    type text,
    sourced_transfers jsonb,
    id text NOT NULL,
    object text,
    created timestamp without time zone,
    amount bigint,
    updated timestamp without time zone,
    _sdc_extracted_at timestamp without time zone,
    _sdc_received_at timestamp without time zone,
    _sdc_batched_at timestamp without time zone,
    _sdc_deleted_at timestamp without time zone,
    _sdc_sequence bigint,
    _sdc_table_version bigint,
    _sdc_sync_started_at bigint
);


ALTER TABLE raw_stripe.balance_transactions OWNER TO uefdt0t8idi0oj;

--
-- TOC entry 602 (class 1259 OID 28278855)
-- Name: charges; Type: TABLE; Schema: raw_stripe; Owner: uefdt0t8idi0oj
--

CREATE TABLE raw_stripe.charges (
    metadata jsonb,
    fraud_details jsonb,
    transfer_group text,
    on_behalf_of text,
    review text,
    failure_message text,
    receipt_email text,
    application_fee_amount bigint,
    disputed boolean,
    payment_method text,
    billing_details jsonb,
    statement_descriptor_suffix text,
    transfer_data jsonb,
    receipt_url text,
    statement_descriptor text,
    source jsonb,
    destination text,
    id text NOT NULL,
    object text,
    outcome jsonb,
    status text,
    currency text,
    created timestamp without time zone,
    "order" text,
    application text,
    refunded boolean,
    receipt_number text,
    livemode boolean,
    captured boolean,
    paid boolean,
    shipping jsonb,
    calculated_statement_descriptor text,
    invoice text,
    amount_captured bigint,
    amount bigint,
    customer text,
    payment_intent text,
    source_transfer text,
    statement_description text,
    refunds jsonb,
    application_fee text,
    card jsonb,
    payment_method_details jsonb,
    balance_transaction text,
    amount_refunded bigint,
    failure_code text,
    dispute text,
    description text,
    updated timestamp without time zone,
    updated_by_event_type text,
    _sdc_extracted_at timestamp without time zone,
    _sdc_received_at timestamp without time zone,
    _sdc_batched_at timestamp without time zone,
    _sdc_deleted_at timestamp without time zone,
    _sdc_sequence bigint,
    _sdc_table_version bigint,
    _sdc_sync_started_at bigint
);


ALTER TABLE raw_stripe.charges OWNER TO uefdt0t8idi0oj;

--
-- TOC entry 611 (class 1259 OID 28278918)
-- Name: coupons; Type: TABLE; Schema: raw_stripe; Owner: uefdt0t8idi0oj
--

CREATE TABLE raw_stripe.coupons (
    metadata jsonb,
    updated_by_event_type text,
    times_redeemed bigint,
    percent_off_precise numeric,
    livemode boolean,
    object text,
    redeem_by timestamp without time zone,
    duration text,
    id text NOT NULL,
    valid boolean,
    currency text,
    duration_in_months bigint,
    name text,
    max_redemptions bigint,
    amount_off bigint,
    created timestamp without time zone,
    percent_off numeric,
    updated timestamp without time zone,
    _sdc_extracted_at timestamp without time zone,
    _sdc_received_at timestamp without time zone,
    _sdc_batched_at timestamp without time zone,
    _sdc_deleted_at timestamp without time zone,
    _sdc_sequence bigint,
    _sdc_table_version bigint,
    _sdc_sync_started_at bigint
);


ALTER TABLE raw_stripe.coupons OWNER TO uefdt0t8idi0oj;

--
-- TOC entry 604 (class 1259 OID 28278869)
-- Name: customers; Type: TABLE; Schema: raw_stripe; Owner: uefdt0t8idi0oj
--

CREATE TABLE raw_stripe.customers (
    metadata jsonb,
    updated_by_event_type text,
    customer_account text,
    preferred_locales jsonb,
    invoice_settings jsonb,
    name text,
    tax_exempt text,
    next_invoice_sequence bigint,
    balance bigint,
    phone text,
    address jsonb,
    shipping jsonb,
    sources text,
    tax_ids text,
    delinquent boolean,
    description text,
    livemode boolean,
    default_source text,
    cards jsonb,
    email text,
    default_card text,
    subscriptions jsonb,
    discount jsonb,
    account_balance bigint,
    currency text,
    id text NOT NULL,
    invoice_prefix text,
    tax_info_verification text,
    object text,
    created timestamp without time zone,
    tax_info text,
    updated timestamp without time zone,
    _sdc_extracted_at timestamp without time zone,
    _sdc_received_at timestamp without time zone,
    _sdc_batched_at timestamp without time zone,
    _sdc_deleted_at timestamp without time zone,
    _sdc_sequence bigint,
    _sdc_table_version bigint,
    _sdc_sync_started_at bigint
);


ALTER TABLE raw_stripe.customers OWNER TO uefdt0t8idi0oj;

--
-- TOC entry 617 (class 1259 OID 28278963)
-- Name: disputes; Type: TABLE; Schema: raw_stripe; Owner: uefdt0t8idi0oj
--

CREATE TABLE raw_stripe.disputes (
    id text NOT NULL,
    updated_by_event_type text,
    object text,
    amount bigint,
    balance_transactions jsonb,
    charge text,
    created timestamp without time zone,
    currency text,
    evidence jsonb,
    evidence_details jsonb,
    is_charge_refundable boolean,
    livemode boolean,
    metadata jsonb,
    reason text,
    status text,
    updated timestamp without time zone,
    _sdc_extracted_at timestamp without time zone,
    _sdc_received_at timestamp without time zone,
    _sdc_batched_at timestamp without time zone,
    _sdc_deleted_at timestamp without time zone,
    _sdc_sequence bigint,
    _sdc_table_version bigint,
    _sdc_sync_started_at bigint
);


ALTER TABLE raw_stripe.disputes OWNER TO uefdt0t8idi0oj;

--
-- TOC entry 603 (class 1259 OID 28278862)
-- Name: events; Type: TABLE; Schema: raw_stripe; Owner: uefdt0t8idi0oj
--

CREATE TABLE raw_stripe.events (
    created timestamp without time zone,
    data jsonb,
    id text NOT NULL,
    api_version text,
    object text,
    livemode boolean,
    pending_webhooks bigint,
    request text,
    type text,
    updated timestamp without time zone,
    _sdc_extracted_at timestamp without time zone,
    _sdc_received_at timestamp without time zone,
    _sdc_batched_at timestamp without time zone,
    _sdc_deleted_at timestamp without time zone,
    _sdc_sequence bigint,
    _sdc_table_version bigint,
    _sdc_sync_started_at bigint
);


ALTER TABLE raw_stripe.events OWNER TO uefdt0t8idi0oj;

--
-- TOC entry 608 (class 1259 OID 28278897)
-- Name: invoice_items; Type: TABLE; Schema: raw_stripe; Owner: uefdt0t8idi0oj
--

CREATE TABLE raw_stripe.invoice_items (
    amount bigint,
    updated_by_event_type text,
    customer_account text,
    metadata jsonb,
    plan jsonb,
    tax_rates jsonb,
    invoice text,
    unit_amount_decimal text,
    period jsonb,
    quantity bigint,
    description text,
    date timestamp without time zone,
    object text,
    subscription text,
    id text NOT NULL,
    livemode boolean,
    discounts jsonb,
    discountable boolean,
    unit_amount bigint,
    currency text,
    customer text,
    proration boolean,
    subscription_item text,
    updated timestamp without time zone,
    _sdc_extracted_at timestamp without time zone,
    _sdc_received_at timestamp without time zone,
    _sdc_batched_at timestamp without time zone,
    _sdc_deleted_at timestamp without time zone,
    _sdc_sequence bigint,
    _sdc_table_version bigint,
    _sdc_sync_started_at bigint
);


ALTER TABLE raw_stripe.invoice_items OWNER TO uefdt0t8idi0oj;

--
-- TOC entry 609 (class 1259 OID 28278904)
-- Name: invoice_line_items; Type: TABLE; Schema: raw_stripe; Owner: uefdt0t8idi0oj
--

CREATE TABLE raw_stripe.invoice_line_items (
    id text NOT NULL,
    invoice text NOT NULL,
    customer_account text,
    subtotal bigint,
    discounts jsonb,
    discount_amounts jsonb,
    tax_amounts jsonb,
    subscription_item text,
    metadata jsonb,
    description text,
    object text,
    discountable boolean,
    quantity bigint,
    amount bigint,
    type text,
    livemode boolean,
    proration boolean,
    proration_details jsonb,
    period jsonb,
    tax_rates jsonb,
    price jsonb,
    subscription text,
    plan jsonb,
    invoice_item text,
    currency text,
    _sdc_extracted_at timestamp without time zone,
    _sdc_received_at timestamp without time zone,
    _sdc_batched_at timestamp without time zone,
    _sdc_deleted_at timestamp without time zone,
    _sdc_sequence bigint,
    _sdc_table_version bigint,
    _sdc_sync_started_at bigint
);


ALTER TABLE raw_stripe.invoice_line_items OWNER TO uefdt0t8idi0oj;

--
-- TOC entry 607 (class 1259 OID 28278890)
-- Name: invoices; Type: TABLE; Schema: raw_stripe; Owner: uefdt0t8idi0oj
--

CREATE TABLE raw_stripe.invoices (
    date timestamp without time zone,
    next_payment_attempt timestamp without time zone,
    updated_by_event_type text,
    customer_tax_exempt text,
    customer_account text,
    footer text,
    customer_name text,
    post_payment_credit_notes_amount bigint,
    created timestamp without time zone,
    status_transitions jsonb,
    default_source text,
    account_country text,
    discounts jsonb,
    account_tax_ids jsonb,
    transfer_data jsonb,
    total_discount_amounts jsonb,
    last_finalization_error jsonb,
    default_tax_rates jsonb,
    customer_tax_ids jsonb,
    total_tax_amounts jsonb,
    customer_address jsonb,
    customer_shipping jsonb,
    customer_phone text,
    payment_intent text,
    customer_email text,
    finalized_at timestamp without time zone,
    pre_payment_credit_notes_amount bigint,
    collection_method text,
    default_payment_method text,
    account_name text,
    tax bigint,
    metadata jsonb,
    charge text,
    description text,
    receipt_number text,
    attempt_count bigint,
    payment text,
    amount_paid bigint,
    due_date timestamp without time zone,
    id text NOT NULL,
    webhooks_delivered_at timestamp without time zone,
    statement_descriptor text,
    hosted_invoice_url text,
    period_end timestamp without time zone,
    amount_remaining bigint,
    tax_percent numeric,
    billing text,
    auto_advance boolean,
    paid boolean,
    discount jsonb,
    number text,
    billing_reason text,
    ending_balance bigint,
    livemode boolean,
    period_start timestamp without time zone,
    attempted boolean,
    closed boolean,
    invoice_pdf text,
    customer text,
    subtotal bigint,
    application_fee bigint,
    application_fee_amount bigint,
    lines jsonb,
    forgiven boolean,
    object text,
    starting_balance bigint,
    amount_due bigint,
    currency text,
    total bigint,
    statement_description text,
    subscription text,
    subscription_details jsonb,
    status text,
    payment_settings jsonb,
    on_behalf_of jsonb,
    custom_fields jsonb,
    paid_out_of_band boolean,
    automatic_tax jsonb,
    quote jsonb,
    updated timestamp without time zone,
    _sdc_extracted_at timestamp without time zone,
    _sdc_received_at timestamp without time zone,
    _sdc_batched_at timestamp without time zone,
    _sdc_deleted_at timestamp without time zone,
    _sdc_sequence bigint,
    _sdc_table_version bigint,
    _sdc_sync_started_at bigint
);


ALTER TABLE raw_stripe.invoices OWNER TO uefdt0t8idi0oj;

--
-- TOC entry 606 (class 1259 OID 28278883)
-- Name: payment_intents; Type: TABLE; Schema: raw_stripe; Owner: uefdt0t8idi0oj
--

CREATE TABLE raw_stripe.payment_intents (
    id text NOT NULL,
    customer_account text,
    amount bigint,
    application text,
    source text,
    status text,
    capture_method text,
    client_secret text,
    latest_charge text,
    currency text,
    customer text,
    description text,
    last_payment_error jsonb,
    metadata jsonb,
    next_action jsonb,
    payment_method text,
    payment_method_types jsonb,
    receipt_email text,
    setup_future_usage text,
    shipping jsonb,
    statement_descriptor text,
    statement_descriptor_suffix text,
    object text,
    amount_capturable bigint,
    amount_received bigint,
    application_fee_amount bigint,
    automatic_payment_methods jsonb,
    canceled_at timestamp without time zone,
    cancellation_reason text,
    invoice text,
    confirmation_method text,
    created timestamp without time zone,
    livemode boolean,
    on_behalf_of text,
    payment_method_options jsonb,
    processing jsonb,
    review text,
    transfer_data jsonb,
    transfer_group text,
    updated timestamp without time zone,
    updated_by_event_type text,
    _sdc_extracted_at timestamp without time zone,
    _sdc_received_at timestamp without time zone,
    _sdc_batched_at timestamp without time zone,
    _sdc_deleted_at timestamp without time zone,
    _sdc_sequence bigint,
    _sdc_table_version bigint,
    _sdc_sync_started_at bigint
);


ALTER TABLE raw_stripe.payment_intents OWNER TO uefdt0t8idi0oj;

--
-- TOC entry 616 (class 1259 OID 28278956)
-- Name: payout_transactions; Type: TABLE; Schema: raw_stripe; Owner: uefdt0t8idi0oj
--

CREATE TABLE raw_stripe.payout_transactions (
    payout_id text,
    id text NOT NULL,
    _sdc_extracted_at timestamp without time zone,
    _sdc_received_at timestamp without time zone,
    _sdc_batched_at timestamp without time zone,
    _sdc_deleted_at timestamp without time zone,
    _sdc_sequence bigint,
    _sdc_table_version bigint,
    _sdc_sync_started_at bigint
);


ALTER TABLE raw_stripe.payout_transactions OWNER TO uefdt0t8idi0oj;

--
-- TOC entry 615 (class 1259 OID 28278949)
-- Name: payouts; Type: TABLE; Schema: raw_stripe; Owner: uefdt0t8idi0oj
--

CREATE TABLE raw_stripe.payouts (
    metadata jsonb,
    failure_code text,
    updated_by_event_type text,
    id text NOT NULL,
    original_payout text,
    reversed_by text,
    statement_description text,
    amount bigint,
    balance_transaction text,
    created timestamp without time zone,
    amount_reversed bigint,
    source_type text,
    bank_account jsonb,
    date timestamp without time zone,
    method text,
    livemode boolean,
    statement_descriptor text,
    failure_message text,
    failure_balance_transaction text,
    recipient text,
    destination text,
    automatic boolean,
    object text,
    status text,
    currency text,
    transfer_group text,
    type text,
    arrival_date timestamp without time zone,
    description text,
    source_transaction text,
    updated timestamp without time zone,
    _sdc_extracted_at timestamp without time zone,
    _sdc_received_at timestamp without time zone,
    _sdc_batched_at timestamp without time zone,
    _sdc_deleted_at timestamp without time zone,
    _sdc_sequence bigint,
    _sdc_table_version bigint,
    _sdc_sync_started_at bigint
);


ALTER TABLE raw_stripe.payouts OWNER TO uefdt0t8idi0oj;

--
-- TOC entry 605 (class 1259 OID 28278876)
-- Name: plans; Type: TABLE; Schema: raw_stripe; Owner: uefdt0t8idi0oj
--

CREATE TABLE raw_stripe.plans (
    nickname text,
    updated_by_event_type text,
    amount_decimal text,
    tiers jsonb,
    object text,
    aggregate_usage text,
    created timestamp without time zone,
    statement_description text,
    product text,
    statement_descriptor text,
    interval_count bigint,
    transform_usage jsonb,
    name text,
    amount bigint,
    "interval" text,
    id text NOT NULL,
    trial_period_days bigint,
    usage_type text,
    active boolean,
    tiers_mode text,
    billing_scheme text,
    livemode boolean,
    currency text,
    metadata jsonb,
    updated timestamp without time zone,
    _sdc_extracted_at timestamp without time zone,
    _sdc_received_at timestamp without time zone,
    _sdc_batched_at timestamp without time zone,
    _sdc_deleted_at timestamp without time zone,
    _sdc_sequence bigint,
    _sdc_table_version bigint,
    _sdc_sync_started_at bigint
);


ALTER TABLE raw_stripe.plans OWNER TO uefdt0t8idi0oj;

--
-- TOC entry 618 (class 1259 OID 28278970)
-- Name: products; Type: TABLE; Schema: raw_stripe; Owner: uefdt0t8idi0oj
--

CREATE TABLE raw_stripe.products (
    id text NOT NULL,
    object text,
    updated_by_event_type text,
    active boolean,
    attributes jsonb,
    caption text,
    created timestamp without time zone,
    deactivate_on jsonb,
    description text,
    images jsonb,
    livemode boolean,
    metadata jsonb,
    name text,
    package_dimensions jsonb,
    shippable boolean,
    statement_descriptor text,
    type text,
    unit_label text,
    updated timestamp without time zone,
    url text,
    tax_code text,
    _sdc_extracted_at timestamp without time zone,
    _sdc_received_at timestamp without time zone,
    _sdc_batched_at timestamp without time zone,
    _sdc_deleted_at timestamp without time zone,
    _sdc_sequence bigint,
    _sdc_table_version bigint,
    _sdc_sync_started_at bigint
);


ALTER TABLE raw_stripe.products OWNER TO uefdt0t8idi0oj;

--
-- TOC entry 613 (class 1259 OID 28278935)
-- Name: subscription_items; Type: TABLE; Schema: raw_stripe; Owner: uefdt0t8idi0oj
--

CREATE TABLE raw_stripe.subscription_items (
    metadata jsonb,
    canceled_at timestamp without time zone,
    current_period_end timestamp without time zone,
    plan jsonb,
    tax_rates jsonb,
    price jsonb,
    subscription text,
    trial_start timestamp without time zone,
    created timestamp without time zone,
    cancel_at_period_end boolean,
    quantity bigint,
    tax_percent numeric,
    current_period_start timestamp without time zone,
    start timestamp without time zone,
    billing_thresholds jsonb,
    discount jsonb,
    application_fee_percent numeric,
    id text NOT NULL,
    status text,
    customer text,
    object text,
    livemode boolean,
    ended_at timestamp without time zone,
    trial_end timestamp without time zone,
    _sdc_extracted_at timestamp without time zone,
    _sdc_received_at timestamp without time zone,
    _sdc_batched_at timestamp without time zone,
    _sdc_deleted_at timestamp without time zone,
    _sdc_sequence bigint,
    _sdc_table_version bigint,
    _sdc_sync_started_at bigint
);


ALTER TABLE raw_stripe.subscription_items OWNER TO uefdt0t8idi0oj;

--
-- TOC entry 612 (class 1259 OID 28278928)
-- Name: subscriptions; Type: TABLE; Schema: raw_stripe; Owner: uefdt0t8idi0oj
--

CREATE TABLE raw_stripe.subscriptions (
    metadata jsonb,
    canceled_at timestamp without time zone,
    customer_account text,
    updated_by_event_type text,
    schedule text,
    next_pending_invoice_item_invoice timestamp without time zone,
    cancel_at timestamp without time zone,
    invoice_customer_balance_settings jsonb,
    pending_invoice_item_interval jsonb,
    pause_collection jsonb,
    transfer_data jsonb,
    latest_invoice text,
    billing_thresholds jsonb,
    pending_setup_intent text,
    start_date timestamp without time zone,
    collection_method text,
    default_source text,
    default_payment_method text,
    livemode boolean,
    start timestamp without time zone,
    items jsonb,
    id text NOT NULL,
    trial_start timestamp without time zone,
    application_fee_percent numeric,
    billing_cycle_anchor timestamp without time zone,
    cancel_at_period_end boolean,
    tax_percent numeric,
    discount jsonb,
    current_period_end timestamp without time zone,
    plan jsonb,
    billing text,
    quantity bigint,
    days_until_due bigint,
    status text,
    created timestamp without time zone,
    ended_at timestamp without time zone,
    customer text,
    current_period_start timestamp without time zone,
    trial_end timestamp without time zone,
    object text,
    updated timestamp without time zone,
    _sdc_extracted_at timestamp without time zone,
    _sdc_received_at timestamp without time zone,
    _sdc_batched_at timestamp without time zone,
    _sdc_deleted_at timestamp without time zone,
    _sdc_sequence bigint,
    _sdc_table_version bigint,
    _sdc_sync_started_at bigint
);


ALTER TABLE raw_stripe.subscriptions OWNER TO uefdt0t8idi0oj;

--
-- TOC entry 619 (class 1259 OID 28278977)
-- Name: transfer_reversals; Type: TABLE; Schema: raw_stripe; Owner: uefdt0t8idi0oj
--

CREATE TABLE raw_stripe.transfer_reversals (
    id text NOT NULL,
    amount bigint,
    currency text,
    metadata jsonb,
    transfer text,
    object text,
    balance_transaction text,
    created timestamp without time zone,
    destination_payment_refund text,
    source_refund text,
    _sdc_extracted_at timestamp without time zone,
    _sdc_received_at timestamp without time zone,
    _sdc_batched_at timestamp without time zone,
    _sdc_deleted_at timestamp without time zone,
    _sdc_sequence bigint,
    _sdc_table_version bigint,
    _sdc_sync_started_at bigint
);


ALTER TABLE raw_stripe.transfer_reversals OWNER TO uefdt0t8idi0oj;

--
-- TOC entry 610 (class 1259 OID 28278911)
-- Name: transfers; Type: TABLE; Schema: raw_stripe; Owner: uefdt0t8idi0oj
--

CREATE TABLE raw_stripe.transfers (
    metadata jsonb,
    updated_by_event_type text,
    reversals jsonb,
    id text NOT NULL,
    statement_description text,
    amount bigint,
    balance_transaction text,
    reversed boolean,
    created timestamp without time zone,
    amount_reversed bigint,
    source_type text,
    source_transaction text,
    date timestamp without time zone,
    livemode boolean,
    statement_descriptor text,
    failure_balance_transaction text,
    recipient text,
    destination text,
    automatic boolean,
    object text,
    currency text,
    transfer_group text,
    arrival_date timestamp without time zone,
    description text,
    updated timestamp without time zone,
    _sdc_extracted_at timestamp without time zone,
    _sdc_received_at timestamp without time zone,
    _sdc_batched_at timestamp without time zone,
    _sdc_deleted_at timestamp without time zone,
    _sdc_sequence bigint,
    _sdc_table_version bigint,
    _sdc_sync_started_at bigint
);


ALTER TABLE raw_stripe.transfers OWNER TO uefdt0t8idi0oj;

--
-- TOC entry 4762 (class 2606 OID 28278948)
-- Name: balance_transactions balance_transactions_pkey; Type: CONSTRAINT; Schema: raw_stripe; Owner: uefdt0t8idi0oj
--

ALTER TABLE ONLY raw_stripe.balance_transactions
    ADD CONSTRAINT balance_transactions_pkey PRIMARY KEY (id);


--
-- TOC entry 4738 (class 2606 OID 28278861)
-- Name: charges charges_pkey; Type: CONSTRAINT; Schema: raw_stripe; Owner: uefdt0t8idi0oj
--

ALTER TABLE ONLY raw_stripe.charges
    ADD CONSTRAINT charges_pkey PRIMARY KEY (id);


--
-- TOC entry 4756 (class 2606 OID 28278924)
-- Name: coupons coupons_pkey; Type: CONSTRAINT; Schema: raw_stripe; Owner: uefdt0t8idi0oj
--

ALTER TABLE ONLY raw_stripe.coupons
    ADD CONSTRAINT coupons_pkey PRIMARY KEY (id);


--
-- TOC entry 4742 (class 2606 OID 28278875)
-- Name: customers customers_pkey; Type: CONSTRAINT; Schema: raw_stripe; Owner: uefdt0t8idi0oj
--

ALTER TABLE ONLY raw_stripe.customers
    ADD CONSTRAINT customers_pkey PRIMARY KEY (id);


--
-- TOC entry 4768 (class 2606 OID 28278969)
-- Name: disputes disputes_pkey; Type: CONSTRAINT; Schema: raw_stripe; Owner: uefdt0t8idi0oj
--

ALTER TABLE ONLY raw_stripe.disputes
    ADD CONSTRAINT disputes_pkey PRIMARY KEY (id);


--
-- TOC entry 4740 (class 2606 OID 28278868)
-- Name: events events_pkey; Type: CONSTRAINT; Schema: raw_stripe; Owner: uefdt0t8idi0oj
--

ALTER TABLE ONLY raw_stripe.events
    ADD CONSTRAINT events_pkey PRIMARY KEY (id);


--
-- TOC entry 4750 (class 2606 OID 28278903)
-- Name: invoice_items invoice_items_pkey; Type: CONSTRAINT; Schema: raw_stripe; Owner: uefdt0t8idi0oj
--

ALTER TABLE ONLY raw_stripe.invoice_items
    ADD CONSTRAINT invoice_items_pkey PRIMARY KEY (id);


--
-- TOC entry 4752 (class 2606 OID 28278910)
-- Name: invoice_line_items invoice_line_items_pkey; Type: CONSTRAINT; Schema: raw_stripe; Owner: uefdt0t8idi0oj
--

ALTER TABLE ONLY raw_stripe.invoice_line_items
    ADD CONSTRAINT invoice_line_items_pkey PRIMARY KEY (id, invoice);


--
-- TOC entry 4748 (class 2606 OID 28278896)
-- Name: invoices invoices_pkey; Type: CONSTRAINT; Schema: raw_stripe; Owner: uefdt0t8idi0oj
--

ALTER TABLE ONLY raw_stripe.invoices
    ADD CONSTRAINT invoices_pkey PRIMARY KEY (id);


--
-- TOC entry 4746 (class 2606 OID 28278889)
-- Name: payment_intents payment_intents_pkey; Type: CONSTRAINT; Schema: raw_stripe; Owner: uefdt0t8idi0oj
--

ALTER TABLE ONLY raw_stripe.payment_intents
    ADD CONSTRAINT payment_intents_pkey PRIMARY KEY (id);


--
-- TOC entry 4766 (class 2606 OID 28278962)
-- Name: payout_transactions payout_transactions_pkey; Type: CONSTRAINT; Schema: raw_stripe; Owner: uefdt0t8idi0oj
--

ALTER TABLE ONLY raw_stripe.payout_transactions
    ADD CONSTRAINT payout_transactions_pkey PRIMARY KEY (id);


--
-- TOC entry 4764 (class 2606 OID 28278955)
-- Name: payouts payouts_pkey; Type: CONSTRAINT; Schema: raw_stripe; Owner: uefdt0t8idi0oj
--

ALTER TABLE ONLY raw_stripe.payouts
    ADD CONSTRAINT payouts_pkey PRIMARY KEY (id);


--
-- TOC entry 4744 (class 2606 OID 28278882)
-- Name: plans plans_pkey; Type: CONSTRAINT; Schema: raw_stripe; Owner: uefdt0t8idi0oj
--

ALTER TABLE ONLY raw_stripe.plans
    ADD CONSTRAINT plans_pkey PRIMARY KEY (id);


--
-- TOC entry 4770 (class 2606 OID 28278976)
-- Name: products products_pkey; Type: CONSTRAINT; Schema: raw_stripe; Owner: uefdt0t8idi0oj
--

ALTER TABLE ONLY raw_stripe.products
    ADD CONSTRAINT products_pkey PRIMARY KEY (id);


--
-- TOC entry 4760 (class 2606 OID 28278941)
-- Name: subscription_items subscription_items_pkey; Type: CONSTRAINT; Schema: raw_stripe; Owner: uefdt0t8idi0oj
--

ALTER TABLE ONLY raw_stripe.subscription_items
    ADD CONSTRAINT subscription_items_pkey PRIMARY KEY (id);


--
-- TOC entry 4758 (class 2606 OID 28278934)
-- Name: subscriptions subscriptions_pkey; Type: CONSTRAINT; Schema: raw_stripe; Owner: uefdt0t8idi0oj
--

ALTER TABLE ONLY raw_stripe.subscriptions
    ADD CONSTRAINT subscriptions_pkey PRIMARY KEY (id);


--
-- TOC entry 4772 (class 2606 OID 28278983)
-- Name: transfer_reversals transfer_reversals_pkey; Type: CONSTRAINT; Schema: raw_stripe; Owner: uefdt0t8idi0oj
--

ALTER TABLE ONLY raw_stripe.transfer_reversals
    ADD CONSTRAINT transfer_reversals_pkey PRIMARY KEY (id);


--
-- TOC entry 4754 (class 2606 OID 28278917)
-- Name: transfers transfers_pkey; Type: CONSTRAINT; Schema: raw_stripe; Owner: uefdt0t8idi0oj
--

ALTER TABLE ONLY raw_stripe.transfers
    ADD CONSTRAINT transfers_pkey PRIMARY KEY (id);


-- Completed on 2026-01-19 16:27:12

--
-- PostgreSQL database dump complete
--

