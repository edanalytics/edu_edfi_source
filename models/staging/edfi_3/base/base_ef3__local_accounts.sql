with local_accounts as (
     {{  edu_edfi_source.source_edfi3('local_accounts')  }} 
),
renamed as (
    select
        tenant_code,
        api_year,
        pull_timestamp,
        file_row_number,
        filename,
        is_deleted,
        v:id::string as record_guid,
        v:accountIdentifier::string as account_identifier,
        v:fiscalYear::int as fiscal_year,
        v:chartOfAccountReference as chart_of_account_reference,
        v:chartOfAccountReference:accountIdentifier::string as chart_of_account_account_identifier,
        v:chartOfAccountReference:educationOrganizationId::bigint as chart_of_account_education_organization_id,
        v:chartOfAccountReference:fiscalYear::int as chart_of_account_fiscal_year,
        v:educationOrganizationReference as education_organization_reference,
        v:educationOrganizationReference:educationOrganizationId::bigint as education_organization_education_organization_id,
        v:accountName::string as account_name,
        v:reportingTags::array as v_reporting_tags,
        v:_lastModifiedDate::datetime as _last_modified_date
    from local_accounts
)
select * from renamed