with stage_leas as (
    select * from {{ ref('stg_ef3__local_education_agencies') }}
),
flattened as (
    select
        tenant_code,
        api_year,
        k_lea,
        {{ extract_descriptor('value:educationOrganizationIdentificationSystemDescriptor::string') }} as id_system,
        value:identificationCode::string as id_code
    from stage_leas
        {{ json_flatten('v_identification_codes') }}
)
select * from flattened
