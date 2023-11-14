with stg_service_centers as (
    select * from {{ ref('stg_ef3__education_service_centers') }}
),
flattened as (
    select 
        tenant_code,
        api_year,
        k_esc,
        {{ extract_descriptor('value:educationOrganizationIdentificationSystemDescriptor::string') }} as id_system,
        value:identificationCode::string as id_code
    from stg_service_centers,
        lateral flatten(input => v_identification_codes)
)
select * from flattened