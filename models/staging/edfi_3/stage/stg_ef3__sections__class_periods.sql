with stg_sections as (
    select * from {{ ref('stg_ef3__sections') }}
),
flattened as (
    select 
        tenant_code,
        api_year,
        k_course_section,
        {{ gen_skey('k_class_period', alt_ref='value:classPeriodReference') }}
    from stg_sections
        {{ json_flatten('v_class_periods') }}
)
select * from flattened
