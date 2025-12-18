with candidates as (
    select * from {{ ref('stg_tpdm__candidates') }}
),
flattened as (
    select 
        tenant_code,
        api_year,
        k_candidate,
        k_candidate_xyear,
        {{ extract_descriptor('value:otherNameTypeDescriptor::varchar') }} as other_name_type,
        value:personalTitlePrefix::varchar as personal_title_prefix,
        value:firstName::varchar as first_name,
        value:middleName::varchar as middle_name,
        value:lastSurname::varchar as last_surname,
        value:generationCodeSuffix::varchar as generation_code_suffix
    from candidates
        {{ json_flatten('v_other_names') }}
)

select * from flattened
