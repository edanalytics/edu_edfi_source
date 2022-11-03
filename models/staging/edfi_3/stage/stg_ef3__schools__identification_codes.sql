with stage_schools as (
    select * from {{ ref('stg_ef3__schools') }}
),
flattened as (
    select 
        tenant_code,
        api_year,
        k_school,
        {{ extract_descriptor('value:educationOrganizationIdentificationSystemDescriptor::string') }} as id_system,
        value:identificationCode::string as id_code
    from stage_schools
        , lateral flatten(input=>v_identification_codes)
)
select * from flattened