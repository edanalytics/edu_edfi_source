with base_bell_schedules as (
    select * from {{ ref('base_ef3__bell_schedules') }}
    where not is_deleted
),
keyed as (
    select 
        {{ dbt_utils.generate_surrogate_key(
            ['tenant_code',
            'api_year',
            'lower(bell_schedule_name)',
            'school_id']
        ) }} as k_bell_schedule, 
        {{ gen_skey('k_school') }},
        api_year as school_year,
        base_bell_schedules.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_bell_schedules
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_bell_schedule',
            order_by='pull_timestamp desc')
    }}
)
select * from deduped
order by tenant_code, api_year desc, school_id, bell_schedule_name
