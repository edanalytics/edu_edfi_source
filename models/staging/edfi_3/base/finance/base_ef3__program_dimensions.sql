with program_dimensions as (
    {{ source_edfi3('program_dimensions') }}
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
        v:code::string as program_dimension_code,
        v:fiscalYear::int as program_dimension_fiscal_year,
        v:codeName::string as program_dimension_code_name,
        -- nested lists
        v:reportingTags::array as v_reporting_tags,

        v:_lastModifiedDate::datetime as _last_modified_date,
        -- edfi extensions
        v:_ext as v_ext
    from program_dimensions
)
select * from renamed
