with professional_dev_events as (
    select * from {{ ref('base_tpdm__professional_development_events') }}
),
keyed as (
    select
        {{ dbt_utils.generate_surrogate_key(
            [
                'tenant_code',
                'api_year',
                'lower(namespace)',
                'lower(professional_development_title)'
            ]
        ) }} as k_professional_development_event,
        professional_dev_events.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from professional_dev_events
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_professional_development_event',
            order_by='last_modified_timestamp desc, pull_timestamp desc'
        )
    }}
)
select * from deduped
where not is_deleted