with persons as (
    {{ source_edfi3('persons') }}
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

        v:id::string       as record_guid,
        v:personId::string as person_id,
        -- descriptors
        {{ extract_descriptor('v:sourceSystemDescriptor::string') }} as source_system
    from persons
)
select * from renamed
