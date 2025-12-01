with base_idea_events as (
    select * from {{ ref('base_sedm__idea_events') }}
),

keyed as (
    select 
        {{ dbt_utils.generate_surrogate_key(
            [
                'tenant_code',
                'api_year',
                'idea_event_id',
                'student_unique_id'
                'idea_event'
            ]
        ) }} as k_idea_event,
        {{ gen_skey('k_student') }},
        {{ gen_skey('k_student_xyear') }},
        base_idea_events.*
        {{ extract_extension(model_name=this.name, flatten=True) }}

    from base_idea_events
),

deduped as (
    {{ dbt_utils.deduplicate(
        relation='keyed',
        partition_by='k_idea_event',
        order_by='last_modified_timestamp desc, pull_timestamp desc'
    ) }}
)

select * from deduped
where not is_deleted
