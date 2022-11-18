with descriptors as (
    {{ source_edfi3('_descriptors') }}
),
renamed as (
    select 
        tenant_code, 
        api_year,
        pull_timestamp, 
        name as descriptor_name, 
        v:id::string                   as record_guid,
        v:codeValue::string            as code_value,
        v:namespace::string            as namespace,
        v:description::string          as description,
        v:shortDescription::string     as short_description
    from descriptors
)
select * 
from renamed