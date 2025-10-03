with local_actuals as (
     {{  edu_edfi_source.source_edfi3('local_actuals')  }} 
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
        v:asOfDate::date as as_of_date,
        v:localAccountReference as local_account_reference,
        v:localAccountReference:accountIdentifier::string as local_account_account_identifier,
        v:localAccountReference:educationOrganizationId::bigint as local_account_education_organization_id,
        v:localAccountReference:fiscalYear::int as local_account_fiscal_year,
        v:amount::float as amount,
        {{  edu_edfi_source.extract_descriptor('v:financialCollectionDescriptor::string')  }}  as financial_collection,
        v:_lastModifiedDate::datetime as _last_modified_date
    from local_actuals
)
select * from renamed