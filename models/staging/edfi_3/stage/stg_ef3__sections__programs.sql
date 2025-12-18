with stg_sections as (
    select * from {{ ref('stg_ef3__sections') }}
),
flattened as (
    select
        tenant_code,
        api_year,
        k_course_offering,
        k_course_section,
        {{ gen_skey('k_program', alt_ref='value:programReference') }}
    from stg_sections
        {{ json_flatten('v_programs') }}
)
select * from flattened
