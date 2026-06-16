CREATE SCHEMA IF NOT EXISTS banking_source;

CREATE TABLE IF NOT EXISTS banking_source.customers (
    customer_id             TEXT PRIMARY KEY,
    registration_time       TIMESTAMPTZ NOT NULL,
    first_name              TEXT NOT NULL,
    last_name               TEXT NOT NULL,
    gender                  CHAR(1) NOT NULL,
    birth_year              INTEGER NOT NULL,
    email                   TEXT NOT NULL,
    phone_number            TEXT NOT NULL,
    country                 CHAR(2) NOT NULL,
    city                    TEXT NOT NULL,
    customer_segment        TEXT NOT NULL,
    customer_status         TEXT NOT NULL,
    source_system           TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS banking_source.accounts (
    account_id                      TEXT PRIMARY KEY,
    customer_id                     TEXT NOT NULL,
    opened_at                       TIMESTAMPTZ NOT NULL,
    account_type                    TEXT NOT NULL,
    currency                        CHAR(3) NOT NULL,
    account_status                  TEXT NOT NULL,
    daily_card_limit                NUMERIC(14,2) NOT NULL,
    daily_transfer_limit            NUMERIC(14,2) NOT NULL,
    daily_cash_withdrawal_limit     NUMERIC(14,2) NOT NULL,
    source_system                   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS banking_source.merchants (
    merchant_id            TEXT PRIMARY KEY,
    merchant_name          TEXT NOT NULL,
    merchant_category      TEXT NOT NULL,
    city                   TEXT NOT NULL,
    country                CHAR(2) NOT NULL,
    terminal_id            TEXT NOT NULL,
    merchant_status        TEXT NOT NULL,
    source_system          TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS banking_source.atms (
    atm_id                 TEXT PRIMARY KEY,
    operator               TEXT NOT NULL,
    city                   TEXT NOT NULL,
    country                CHAR(2) NOT NULL,
    location_type          TEXT NOT NULL,
    atm_status             TEXT NOT NULL,
    source_system          TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS banking_source.transactions (
    transaction_id             TEXT PRIMARY KEY,
    event_time                 TIMESTAMPTZ NOT NULL,
    customer_id                TEXT NOT NULL,
    account_id                 TEXT NOT NULL,
    merchant_id                TEXT,
    atm_id                     TEXT,
    payment_method             TEXT NOT NULL,
    channel                    TEXT NOT NULL,
    amount                     NUMERIC(14,2) NOT NULL,
    currency                   CHAR(3) NOT NULL,
    authorization_status       TEXT NOT NULL,
    decline_reason             TEXT,
    device_id                  TEXT NOT NULL,
    device_type                TEXT NOT NULL,
    ip_country                 CHAR(2) NOT NULL,
    city                       TEXT NOT NULL,
    risk_score                 INTEGER NOT NULL,
    is_fraud_suspected         BOOLEAN NOT NULL,
    fraud_rule_hit             TEXT,
    source_system              TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS banking_source.kyc_aml_checks (
    kyc_event_id                   TEXT PRIMARY KEY,
    customer_id                    TEXT NOT NULL,
    event_time                     TIMESTAMPTZ NOT NULL,
    kyc_status                     TEXT NOT NULL,
    aml_risk_score                 INTEGER NOT NULL,
    aml_risk_level                 TEXT NOT NULL,
    pep_flag                       BOOLEAN NOT NULL,
    sanctions_screening_status     TEXT NOT NULL,
    source_system                  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS banking_source.support_cases (
    support_case_id        TEXT PRIMARY KEY,
    customer_id            TEXT NOT NULL,
    created_at             TIMESTAMPTZ NOT NULL,
    case_type              TEXT NOT NULL,
    case_status            TEXT NOT NULL,
    priority               TEXT NOT NULL,
    source_system          TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS banking_source.chargeback_cases (
    chargeback_id          TEXT PRIMARY KEY,
    transaction_id         TEXT NOT NULL,
    customer_id            TEXT NOT NULL,
    merchant_id            TEXT,
    case_opened_at         TIMESTAMPTZ NOT NULL,
    reason_code            TEXT NOT NULL,
    case_status            TEXT NOT NULL,
    amount                 NUMERIC(14,2) NOT NULL,
    currency               CHAR(3) NOT NULL,
    source_system          TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS banking_source.customer_risk_scores (
    risk_event_id          TEXT PRIMARY KEY,
    customer_id            TEXT NOT NULL,
    score_time             TIMESTAMPTZ NOT NULL,
    risk_score             INTEGER NOT NULL,
    risk_level             TEXT NOT NULL,
    main_risk_driver       TEXT NOT NULL,
    model_version          TEXT NOT NULL,
    source_system          TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS banking_source.merchant_risk_scores (
    merchant_risk_event_id     TEXT PRIMARY KEY,
    merchant_id                TEXT NOT NULL,
    score_time                 TIMESTAMPTZ NOT NULL,
    merchant_risk_score        INTEGER NOT NULL,
    merchant_risk_level        TEXT NOT NULL,
    main_risk_driver           TEXT NOT NULL,
    source_system              TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS banking_source.device_blacklist (
    blacklist_event_id     TEXT PRIMARY KEY,
    device_id              TEXT NOT NULL,
    event_time             TIMESTAMPTZ NOT NULL,
    reason                 TEXT NOT NULL,
    severity               TEXT NOT NULL,
    active_flag            BOOLEAN NOT NULL,
    source_system          TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS banking_source.daily_account_balances (
    balance_id             TEXT PRIMARY KEY,
    account_id             TEXT NOT NULL,
    balance_date           DATE NOT NULL,
    opening_balance        NUMERIC(16,2) NOT NULL,
    closing_balance        NUMERIC(16,2) NOT NULL,
    available_balance      NUMERIC(16,2) NOT NULL,
    currency               CHAR(3) NOT NULL,
    source_system          TEXT NOT NULL,
    created_at             TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS banking_source.stream_events (
    event_id               TEXT PRIMARY KEY,
    event_time             TIMESTAMPTZ NOT NULL,
    event_type             TEXT NOT NULL,
    source_system          TEXT NOT NULL,
    entity_type            TEXT NOT NULL,
    entity_id              TEXT NOT NULL,
    payload_json           JSONB NOT NULL,
    replayed_flag          BOOLEAN NOT NULL DEFAULT FALSE,
    replayed_at            TIMESTAMPTZ
);
