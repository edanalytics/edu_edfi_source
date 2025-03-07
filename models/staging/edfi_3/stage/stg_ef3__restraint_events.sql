with base_restraint_events as (
    select * from {{ ref('base_ef3__restraint_events') }}
    where not is_deleted
),
keyed as (
    select
        {{ gen_skey('k_student') }},
        {{ gen_skey('k_student_xyear') }},
        {{ gen_skey('k_school') }},
        {{ dbt_utils.generate_surrogate_key(
            [
                'tenant_code',
                'api_year',
                'event_date',
                'school_id',
                'api_year',
                'restraint_event_identifier'
            ]
        ) }} as k_restraint_event,
        api_year as school_year,
        *
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_restraint_events
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_student, k_restraint_event, educational_environment',
            order_by='pull_timestamp desc'
        )
    }}
)
select * from deduped