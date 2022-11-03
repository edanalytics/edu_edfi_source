with stg_bell_schedules as (
    select * from {{ ref('stg_ef3__bell_schedules') }}
),
flattened as (
    select 
        tenant_code,
        api_year,
        k_bell_schedule,
        k_school,
        {{ gen_skey('k_class_period', 'value:classPeriodReference') }},
        value:classPeriodReference:classPeriodName::string as class_period_name,
        value:classPeriodReference:schoolId::int           as class_period_school_id
    from stg_bell_schedules,
        lateral flatten(input => v_class_periods)
)
select * from flattened