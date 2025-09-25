with base_path_milestones as (
    select * from {{ ref('base_tpdm__path_milestones') }}
),
keyed as (
    select
        {{ dbt_utils.generate_surrogate_key(
            [
                'tenant_code',
                'api_year',
                'lower(path_milestone_name)',
                'lower(path_milestone_type)'
            ]
        ) }} as k_path_milestone,
        base_path_milestones.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_path_milestones
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_path_milestone',
            order_by='last_modified_timestamp desc, pull_timestamp desc'
        )
    }}
)
select * from deduped
where not is_deleted