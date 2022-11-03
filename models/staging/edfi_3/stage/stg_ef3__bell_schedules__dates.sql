with stg_bell_schedules as (
    select * from {{ ref('stg_ef3__bell_schedules') }}
),
flattened as (
    select 
        tenant_code,
        api_year,
        k_bell_schedule,
        k_school,
        value:date::date as calendar_date
    from stg_bell_schedules,
        lateral flatten(input => v_dates)
)
select * from flattened