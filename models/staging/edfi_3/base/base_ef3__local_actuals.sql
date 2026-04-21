with local_actuals as (
    {{ source_edfi3('local_actuals') }}
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
        v:localAccountReference:accountIdentifier::string as local_account_identifier,
        v:localAccountReference:educationOrganizationId::bigint as ed_org_id,
        v:localAccountReference:link:rel::string as ed_org_type,
        v:localAccountReference:fiscalYear::int as fiscal_year,
        v:asOfDate::date as as_of_date,
        -- value columns
        v:amount::float as amount,
        -- descriptors
        {{ extract_descriptor('v:financialCollectionDescriptor::string') }} as financial_collection,
        -- references
        v:localAccountReference as local_account_reference,

        v:_lastModifiedDate::datetime as _last_modified_date,
        -- edfi extensions
        v:_ext as v_ext
    from local_actuals
)
select * from renamed
