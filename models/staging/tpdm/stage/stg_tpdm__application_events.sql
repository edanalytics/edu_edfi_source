with base_app_events as (
    select * from {{ ref('base_tpdm__application_events')}}
),
keyed as (
    select
        {{ gen_skey('k_application') }},
        base_app_events.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_app_events
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_application, sequence_number, event_date, application_event_type',
            order_by='last_modified_timestamp desc, pull_timestamp desc'
        )
    }}
)
select * from deduped
where not is_deleted