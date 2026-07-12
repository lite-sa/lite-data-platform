-- Disposable local stand-in for 4 real LiteCore tables, so the dlt
-- PG -> GCS -> BQ wiring can be tested before a real replica connection
-- exists. Schema copied from LiteCore's own Liquibase changelogs (not
-- guessed):
--   payment_v2.payments, payment_v2.payment_operations
--     <- apps/payment-v2-service/migrations/postgres/changelogs/0001-init-schema.yaml
--        (+ 0006-add-payment-link-fields-to-payments.yaml)
--   user.merchants
--     <- apps/user-service/migrations/postgres/changelogs/0001-init-schema.yaml
--   business_management.business_entities
--     <- apps/business-management-service/migrations/postgres/changelogs/0001-init-schema.yaml
--        (+ 0003-add-wathq-name-email-fields.yaml)
-- All 3 tables are owned by 3 *different* services -- see README.md for why
-- that matters for the real connection.

CREATE SCHEMA IF NOT EXISTS payment_v2;
CREATE SCHEMA IF NOT EXISTS "user";
CREATE SCHEMA IF NOT EXISTS business_management;

CREATE TABLE payment_v2.payments (
  id VARCHAR(36) PRIMARY KEY,
  merchant_id VARCHAR(36) NOT NULL,
  amount BIGINT NOT NULL,
  currency VARCHAR(3) NOT NULL,
  status VARCHAR(64) NOT NULL DEFAULT 'CREATED',
  processing_type VARCHAR(64),
  capture_mode VARCHAR(64),
  payment_instrument_id VARCHAR(128),
  risk JSONB,
  customer JSONB,
  order_data JSONB,
  device JSONB,
  threeds_input JSONB,
  threeds_result JSONB,
  return_url JSONB,
  metadata JSONB,
  gateway_reference_id VARCHAR(128),
  routing_result JSONB,
  risk_result JSONB,
  payment_method VARCHAR(64),
  channel_id VARCHAR(128) DEFAULT 'default',
  order_id VARCHAR(128),
  payment_link_id UUID,
  payment_link_consumption_id UUID,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE payment_v2.payment_operations (
  id VARCHAR(36) PRIMARY KEY,
  payment_id VARCHAR(36) NOT NULL REFERENCES payment_v2.payments(id),
  idempotency_key VARCHAR(128),
  operation_type VARCHAR(64) NOT NULL,
  status VARCHAR(64) NOT NULL DEFAULT 'PENDING',
  amount BIGINT NOT NULL,
  currency VARCHAR(3) NOT NULL,
  reason TEXT,
  initiated_by VARCHAR(128),
  connector_type VARCHAR(64),
  connector_response_code VARCHAR(64),
  connector_response_message TEXT,
  connector_transaction_id VARCHAR(256),
  http_status_code BIGINT,
  processing_duration_ms BIGINT,
  retry_count BIGINT,
  max_retries BIGINT,
  next_retry_at TIMESTAMPTZ,
  terminal_id VARCHAR(50),
  entry_mode VARCHAR(20),
  rrn VARCHAR(50),
  stan VARCHAR(12),
  reconciliation_status VARCHAR(20),
  external_operation_type VARCHAR(30),
  verification_method VARCHAR(30),
  metadata JSONB,
  raw_provider_request TEXT,
  raw_provider_response TEXT,
  related_operation_id VARCHAR(100),
  reversed_operation_id VARCHAR(100),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- "user" is a reserved word -> always double-quoted.
CREATE TABLE "user".merchants (
  id VARCHAR(36) PRIMARY KEY,
  merchant_id VARCHAR(128) NOT NULL,
  status VARCHAR(255),
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE business_management.business_entities (
  id VARCHAR(36) PRIMARY KEY,
  business_id VARCHAR(128) NOT NULL,
  status VARCHAR(128) NOT NULL,
  cr_number VARCHAR(64),
  wathq_data_retrieved BOOLEAN,
  cr_national_number VARCHAR(255),
  name VARCHAR(255),
  cr_capital NUMERIC,
  company_duration NUMERIC,
  is_main BOOLEAN,
  issue_date_gregorian VARCHAR(255),
  issue_date_hijri VARCHAR(255),
  main_cr_national_number VARCHAR(255),
  main_cr_number VARCHAR(255),
  in_liquidation_process BOOLEAN,
  has_ecommerce BOOLEAN,
  headquarter_city_name VARCHAR(255),
  is_license_based BOOLEAN,
  license_issuer_national_number VARCHAR(255),
  license_issuer_name VARCHAR(255),
  partners_nationality_name VARCHAR(255),
  type VARCHAR(255),
  type_form VARCHAR(255),
  wathq_status VARCHAR(255),
  wathq_status_confirmation_date_gregorian VARCHAR(255),
  wathq_status_confirmation_date_hijri VARCHAR(255),
  wathq_status_reactivation_date_gregorian VARCHAR(255),
  wathq_status_reactivation_date_hijri VARCHAR(255),
  wathq_status_suspension_date_gregorian VARCHAR(255),
  wathq_status_suspension_date_hijri VARCHAR(255),
  wathq_status_deletion_date_gregorian VARCHAR(255),
  wathq_status_deletion_date_hijri VARCHAR(255),
  contact_phone_number VARCHAR(255),
  contact_mobile_number VARCHAR(255),
  contact_email VARCHAR(255),
  website_url VARCHAR(255),
  capital_currency_id NUMERIC,
  capital_currency_name VARCHAR(255),
  contribution_capital JSONB,
  stock_capital JSONB,
  fiscal_year JSONB,
  management_structure_name VARCHAR(255),
  entity_characters VARCHAR(255)[],
  annual_revenue_from BIGINT,
  annual_revenue_to BIGINT,
  nafath_trans_id VARCHAR(64),
  nafath_random VARCHAR(64),
  nafath_request_id VARCHAR(64),
  nafath_initiated_at TIMESTAMPTZ,
  wathq_legal_name VARCHAR(255),
  wathq_contact_email VARCHAR(255),
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL
);

-- Fabricated rows, not real merchant data.
INSERT INTO payment_v2.payments (id, merchant_id, amount, currency, status, payment_method, channel_id, order_id, created_at, updated_at) VALUES
  ('pay_0001', 'merch_0001', 15000,  'SAR', 'CAPTURED', 'CARD', 'default', 'order_0001', now() - interval '2 days', now() - interval '2 days'),
  ('pay_0002', 'merch_0001', 5000,   'SAR', 'CAPTURED', 'CARD', 'default', 'order_0002', now() - interval '1 days', now() - interval '1 days'),
  ('pay_0003', 'merch_0002', 250000, 'SAR', 'CREATED',  'CARD', 'default', 'order_0003', now(), now());

INSERT INTO payment_v2.payment_operations (id, payment_id, operation_type, status, amount, currency, created_at, updated_at) VALUES
  ('op_0001', 'pay_0001', 'AUTHORIZE', 'SUCCESS', 15000, 'SAR', now() - interval '2 days', now() - interval '2 days'),
  ('op_0002', 'pay_0001', 'CAPTURE',   'SUCCESS', 15000, 'SAR', now() - interval '2 days', now() - interval '2 days'),
  ('op_0003', 'pay_0002', 'AUTHORIZE', 'SUCCESS', 5000,  'SAR', now() - interval '1 days', now() - interval '1 days');

INSERT INTO "user".merchants (id, merchant_id, status, created_at, updated_at) VALUES
  ('mch_0001', 'merch_0001', 'ACTIVE', now() - interval '30 days', now() - interval '30 days'),
  ('mch_0002', 'merch_0002', 'ACTIVE', now() - interval '10 days', now() - interval '10 days');

INSERT INTO business_management.business_entities (id, business_id, status, cr_number, name, in_liquidation_process, has_ecommerce, created_at, updated_at) VALUES
  ('be_0001', 'merch_0001', 'ACTIVE', '1010101010', 'Sample Trading Co', false, true,  now() - interval '30 days', now() - interval '30 days'),
  ('be_0002', 'merch_0002', 'ACTIVE', '2020202020', 'Sample Retail Est', false, false, now() - interval '10 days', now() - interval '10 days');
