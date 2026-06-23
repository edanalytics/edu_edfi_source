with balance_sheet_dimensions as (
    {{ source_edfi3('balance_sheet_dimensions') }}
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
        v:code::string as balance_sheet_dimension_code,
        v:fiscalYear::int as balance_sheet_dimension_fiscal_year,
        v:codeName::string as balance_sheet_dimension_code_name,
        -- nested lists
        v:reportingTags::array as v_reporting_tags,

        v:_lastModifiedDate::datetime as _last_modified_date,
        -- edfi extensions
        v:_ext as v_ext
    from balance_sheet_dimensions
)
select * from renamed
