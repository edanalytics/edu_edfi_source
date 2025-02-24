with base_calendars as (
    select * from {{ ref('base_ef3__calendars') }}
    where not is_deleted
),
keyed as (
    select 
        {{ dbt_utils.generate_surrogate_key(
            [
                'tenant_code',
                'lower(calendar_code)',
                'school_id',
                'school_year'
            ]
        ) }} as k_school_calendar,
        {{ gen_skey('k_school') }},
        base_calendars.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_calendars
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_school_calendar',
            order_by='api_year desc, pull_timestamp desc'
        )
    }}
)
select * from deduped