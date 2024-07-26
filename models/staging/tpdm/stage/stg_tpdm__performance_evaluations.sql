with base_performance_evaluations as (
    select * from {{ ref('base_tpdm__performance_evaluations') }}
    where not is_deleted
),
keyed as (
    select
        {{ dbt_utils.surrogate_key(
            [
                'tenant_code',
                'api_year'
            ]
        ) }} as k_performance_evaluation,
        base_performance_evaluations.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_performance_evaluations
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_performance_evaluation',
            order_by='pull_timestamp desc'
        )
    }}
)
select * from deduped
