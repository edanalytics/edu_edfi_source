with base_performance_evaluation_ratings as (
    select * from {{ ref('base_tpdm__performance_evaluation_ratings') }}
    where not is_deleted
),
keyed as (
    select
        {{ dbt_utils.surrogate_key(
            [
                'tenant_code',
                'api_year',
                'ed_org_id',
                'performance_evaluation_title',
                'school_year',
                'person_id'
            ]
        ) }} as k_performance_evaluation_rating,
        base_performance_evaluation_ratings.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_performance_evaluation_ratings
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_performance_evaluation_rating',
            order_by='pull_timestamp desc'
        )
    }}
)
select * from deduped
