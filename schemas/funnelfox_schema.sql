-- Create schema for FunnelFox raw data
CREATE SCHEMA IF NOT EXISTS raw_funnelfox;

-- Funnels table
CREATE TABLE IF NOT EXISTS raw_funnelfox.funnels (
    id TEXT PRIMARY KEY,
    alias TEXT,
    environment TEXT,
    last_published_at TIMESTAMP WITH TIME ZONE,
    status TEXT,
    tags TEXT[],
    title TEXT,
    type TEXT,
    variation_count INTEGER,
    version INTEGER,
    loaded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Products table
CREATE TABLE IF NOT EXISTS raw_funnelfox.products (
    id TEXT PRIMARY KEY,
    data JSONB,
    loaded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Sessions table
CREATE TABLE IF NOT EXISTS raw_funnelfox.sessions (
    id TEXT PRIMARY KEY,
    city TEXT,
    country TEXT,
    created_at TIMESTAMP WITH TIME ZONE,
    funnel_id TEXT,
    funnel_version INTEGER,
    ip TEXT,
    origin TEXT,
    postal TEXT,
    profile_id TEXT,
    user_agent TEXT,
    loaded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (funnel_id) REFERENCES raw_funnelfox.funnels(id)
);

-- Subscriptions table
CREATE TABLE IF NOT EXISTS raw_funnelfox.subscriptions (
    id TEXT PRIMARY KEY,
    billing_interval TEXT,
    billing_interval_count INTEGER,
    created_at TIMESTAMP WITH TIME ZONE,
    currency TEXT,
    funnel_version INTEGER,
    payment_provider TEXT,
    period_ends_at TIMESTAMP WITH TIME ZONE,
    period_starts_at TIMESTAMP WITH TIME ZONE,
    price INTEGER,
    price_usd INTEGER,
    profile_id TEXT,
    psp_id TEXT,
    renews BOOLEAN,
    sandbox BOOLEAN,
    status TEXT,
    updated_at TIMESTAMP WITH TIME ZONE,
    loaded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Profiles table
CREATE TABLE IF NOT EXISTS raw_funnelfox.profiles (
    id TEXT PRIMARY KEY,
    data JSONB,
    loaded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Transactions table
CREATE TABLE IF NOT EXISTS raw_funnelfox.transactions (
    id TEXT PRIMARY KEY,
    data JSONB,
    loaded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Session replies table (form responses from funnel interactions)
CREATE TABLE IF NOT EXISTS raw_funnelfox.session_replies (
    id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL,
    data JSONB,
    loaded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (session_id) REFERENCES raw_funnelfox.sessions(id)
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_sessions_funnel_id ON raw_funnelfox.sessions(funnel_id);
CREATE INDEX IF NOT EXISTS idx_sessions_created_at ON raw_funnelfox.sessions(created_at);
CREATE INDEX IF NOT EXISTS idx_sessions_profile_id ON raw_funnelfox.sessions(profile_id);
CREATE INDEX IF NOT EXISTS idx_subscriptions_status ON raw_funnelfox.subscriptions(status);
CREATE INDEX IF NOT EXISTS idx_subscriptions_created_at ON raw_funnelfox.subscriptions(created_at);
CREATE INDEX IF NOT EXISTS idx_subscriptions_profile_id ON raw_funnelfox.subscriptions(profile_id);
CREATE INDEX IF NOT EXISTS idx_session_replies_session_id ON raw_funnelfox.session_replies(session_id);
