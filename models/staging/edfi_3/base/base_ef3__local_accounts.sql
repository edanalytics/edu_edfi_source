with local_accounts as (
    {{ source_edfi3('local_accounts') }}
),
renamed as (
    select
        tenant_code,
        api_year,
        pull_timestamp,
        last_modified_timestamp,
        file_row_number,
        filename,
        is_deleted,
        v:id::string as record_guid,
        ods_version,
        data_model_version,
        -- key columns
        v:accountIdentifier::string as local_account_identifier,
        v:educationOrganizationReference:educationOrganizationId::bigint as ed_org_id,
        v:educationOrganizationReference:link:rel::string as ed_org_type,
        v:fiscalYear::int as fiscal_year,
        -- value columns
        v:accountName::string as account_name,
        -- references
        v:educationOrganizationReference as education_organization_reference,
        v:chartOfAccountReference as chart_of_account_reference,
        -- nested lists
        v:reportingTags::array as v_reporting_tags,

        v:_lastModifiedDate::datetime as _last_modified_date,
        -- edfi extensions
        v:_ext as v_ext
    from local_accounts
)
select * from renamed
