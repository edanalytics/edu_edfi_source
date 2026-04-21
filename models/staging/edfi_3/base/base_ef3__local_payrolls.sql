with local_payrolls as (
    {{ source_edfi3('local_payrolls') }}
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
        -- key columns (local account + staff identify the row)
        v:localAccountReference:accountIdentifier::string as local_account_identifier,
        v:localAccountReference:educationOrganizationId::bigint as ed_org_id,
        v:localAccountReference:link:rel::string as ed_org_type,
        v:localAccountReference:fiscalYear::int as fiscal_year,
        v:staffReference:staffUniqueId::string as staff_unique_id,
        v:asOfDate::date as as_of_date,
        -- value columns
        coalesce(v:amount::float, v:amountToDate::float) as amount,
        -- descriptors
        {{ extract_descriptor('v:financialCollectionDescriptor::string') }} as financial_collection,
        -- references
        v:localAccountReference as local_account_reference,
        v:staffReference as staff_reference,

        v:_lastModifiedDate::datetime as _last_modified_date,
        -- edfi extensions
        v:_ext as v_ext
    from local_payrolls
)
select * from renamed
