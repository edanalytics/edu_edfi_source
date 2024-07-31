with base_evaluations as (
    select * from {{ ref('base_tpdm__evaluations') }}
    where not is_deleted
),
keyed as (
    select
        {{ dbt_utils.surrogate_key(
            [
                'tenant_code',
                'api_year',
                'ed_org_id',
                'lower(evaluation_period)',
                'lower(evaluation_title)',
                'lower(perfomance_evaluation_title)',
                'lower(performance_evaluation_type)',
                'school_year',
                'lower(academic_term)'
            ]
        ) }} as k_evaluation,
        {{ gen_skey('k_performance_evaluation') }},
        base_evaluations.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_evaluations
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_evaluation',
            order_by='pull_timestamp desc'
        )
    }}
)
select * from deduped
