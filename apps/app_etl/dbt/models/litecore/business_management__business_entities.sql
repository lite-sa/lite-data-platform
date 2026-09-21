-- Litecore mirror of raw business_management__business_entities:
-- the latest version of every row, PII columns removed. The column list is
-- an allowlist: a new source column reaches this table only when added here.
--
-- Left out: contact_phone_number, contact_mobile_number, contact_email,
-- wathq_contact_email, agent_email, sales_email, deleted_by: phone numbers
-- and email addresses; nafath_*: identity-verification artifacts, one a
-- national ID number; high_risk_device_id: a device fingerprint;
-- partners_nationality_name, contribution_capital, stock_capital,
-- fiscal_year, business_sequence, ticket_id, focal_*: shapes never reviewed
-- (trim map); invited_by: caller-typed text, no prod value to review yet;
-- risk_score, aml_risk_score, is_stakeholders_pep_compliant,
-- is_high_risk_country_onboarding: compliance assessments, not for the
-- payments team.
select
    {{
        keep_columns(
            source('litecore', 'business_management__business_entities'),
            [
                'id',
                'business_id',
                'name',
                'wathq_legal_name',
                'status',
                'type',
                'type_form',
                'entity_type_id',
                'product_type',
                'product_opt_ins',
                'mccs',
                'cr_number',
                'cr_national_number',
                'is_main',
                'main_cr_number',
                'main_cr_national_number',
                'cr_capital',
                'capital_currency_id',
                'capital_currency_name',
                'company_duration',
                'issue_date_gregorian',
                'issue_date_hijri',
                'headquarter_city_name',
                'management_structure_name',
                'entity_characters',
                'has_ecommerce',
                'website_url',
                'in_liquidation_process',
                'is_license_based',
                'license_issuer_national_number',
                'license_issuer_name',
                'wathq_data_retrieved',
                'wathq_status',
                'wathq_status_confirmation_date_gregorian',
                'wathq_status_confirmation_date_hijri',
                'wathq_status_reactivation_date_gregorian',
                'wathq_status_reactivation_date_hijri',
                'wathq_status_suspension_date_gregorian',
                'wathq_status_suspension_date_hijri',
                'wathq_status_deletion_date_gregorian',
                'wathq_status_deletion_date_hijri',
                'annual_revenue_from',
                'annual_revenue_to',
                'number_of_employees_from',
                'number_of_employees_to',
                'is_pci_compliant',
                'is_manually_created',
                'kyb_approved_at',
                'is_deleted',
                'deleted_at',
                'created_at',
                'updated_at',
            ]
        )
    }}
from {{ source('litecore', 'business_management__business_entities') }}
qualify
    row_number() over (
        partition by id order by updated_at desc, _dlt_load_id desc
    )
    = 1
