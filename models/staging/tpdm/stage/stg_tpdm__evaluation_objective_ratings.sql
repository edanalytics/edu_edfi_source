with base_evaluation_objective_ratings as (
    select * from {{ ref('base_tpdm__evaluation_objective_ratings') }}
    where not is_deleted
),
keyed as (
    select
        {{ dbt_utils.surrogate_key(
            [
                'tenant_code',
                'api_year'
            ]
        ) }} as k_evaluation_objective_ratings,
        base_evaluation_objective_ratings.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_evaluation_objective_ratings
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_evaluation_objective_ratings',
            order_by='pull_timestamp desc'
        )
    }}
)
select * from deduped
