with base_app_events as (
    select * from {{ ref('base_tpdm__application_events')}}
),
keyed as (
    select
        {{ gen_skey('k_application') }},
        {{ gen_skey('k_ed_org') }},
        *
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_app_events
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_school, k_student, restraint_event_identifier',
            order_by='last_modified_timestamp desc, pull_timestamp desc'
        )
    }}
)
select * from deduped
where not is_deleted