with function_dimensions as (
     {{  edu_edfi_source.source_edfi3('function_dimensions')  }} 
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
        v:code::string as code,
        v:fiscalYear::int as fiscal_year,
        v:codeName::string as code_name,
        v:reportingTags::array as v_reporting_tags,
        v:_lastModifiedDate::datetime as _last_modified_date
    from function_dimensions
)
select * from renamed