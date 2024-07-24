with peoples as (
    {{ source_edfi3('peoples') }}
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

        v:id::string    as record_guid,
        v:personId::int as person_id,
        -- descriptors
        {{ extract_descriptor('v:sourceSystemDescriptor::string') }} as source_system
    from peoples
)
select * from renamed
