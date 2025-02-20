with people as (
    {{ source_edfi3('people') }}
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
        -- identity components
        v:personId::int as person_id,
        {{ extract_descriptor('v:sourceSystemDescriptor::string') }} as source_system
    from people
)
select * from renamed
