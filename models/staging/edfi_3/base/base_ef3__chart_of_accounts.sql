with chart_of_accounts as (
    {{ source_edfi3('chart_of_accounts') }}
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
        v:accountIdentifier::string as account_identifier,
        v:fiscalYear::int as fiscal_year,
        v:accountName::string as account_name,
        -- education organization reference
        v:educationOrganizationReference as education_organization_reference,
        v:educationOrganizationReference:educationOrganizationId::bigint as ed_org_id,
        v:educationOrganizationReference:link:rel::string as ed_org_type,
        -- descriptors
        {{ extract_descriptor('v:accountTypeDescriptor::string') }} as account_type,

        -- references
        v:balanceSheetDimensionReference as balance_sheet_dimension_reference,
        v:functionDimensionReference as function_dimension_reference,
        v:fundDimensionReference as fund_dimension_reference,
        v:objectDimensionReference as object_dimension_reference,
        v:operationalUnitDimensionReference as operational_unit_dimension_reference,
        v:programDimensionReference as program_dimension_reference,
        v:projectDimensionReference as project_dimension_reference,
        v:sourceDimensionReference as source_dimension_reference,

        -- nested lists
        v:reportingTags::array as v_reporting_tags,

        v:_lastModifiedDate::datetime as _last_modified_date,
        -- edfi extensions
        v:_ext as v_ext
    from chart_of_accounts
)
select * from renamed
