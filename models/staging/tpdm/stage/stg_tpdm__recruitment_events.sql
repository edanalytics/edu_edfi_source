with base_recruitment_events as (
    select * from {{ ref('base_tpdm__recruitment_events') }}
),
keyed as (
    select
        {{ dbt_utils.generate_surrogate_key(
            [
                'tenant_code',
                'api_year',
                'ed_org_id',
                'event_date',
                'lower(event_title)',
            ]
        ) }} as k_recruitment_event,
        {{ gen_skey('k_education_organization') }},
        base_recruitment_events.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_recruitment_events
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_recruitment_event',
            order_by='last_modified_timestamp desc, pull_timestamp desc'
        )
    }}
)
select * from deduped
where not is_deleted