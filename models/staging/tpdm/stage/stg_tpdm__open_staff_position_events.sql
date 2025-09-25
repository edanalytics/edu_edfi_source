with open_staff_position_events as (
    select * from {{ ref('base_tpdm__open_staff_position_events')}}
),
keyed as (
    select
        {{ gen_skey('k_open_staff_position') }},
        open_staff_position_events.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from open_staff_position_events
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_open_staff_position, event_date, open_staff_position_event_type',
            order_by='last_modified_timestamp desc, pull_timestamp desc'
        )
    }}
)
select * from deduped
where not is_deleted