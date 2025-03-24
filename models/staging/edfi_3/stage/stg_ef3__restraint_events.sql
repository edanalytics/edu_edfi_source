with base_restraint_events as (
    select * from {{ ref('base_ef3__restraint_events') }}
),
keyed as (
    select
        {{ gen_skey('k_student') }},
        {{ gen_skey('k_student_xyear') }},
        {{ gen_skey('k_school') }},
        api_year as school_year,
        *
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_restraint_events
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_school, k_student, restraint_event_identifier',
            order_by='last_modified_timestamp desc'
        )
    }}
)
select * from deduped
where not is_deleted
