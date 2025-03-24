with base_evaluation_objectives as (
    select * from {{ ref('base_tpdm__evaluation_objectives') }}
),
keyed as (
    select
        {{ dbt_utils.generate_surrogate_key(
            [
                'tenant_code',
                'api_year',
                'ed_org_id',
                'lower(evaluation_objective_title)',
                'lower(evaluation_period)',
                'lower(evaluation_title)',
                'lower(perfomance_evaluation_title)',
                'lower(performance_evaluation_type)',
                'school_year',
                'lower(academic_term)'
            ]
        ) }} as k_evaluation_objective,
        {{ gen_skey('k_evaluation') }},
        base_evaluation_objectives.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_evaluation_objectives
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_evaluation_objective',
            order_by='last_modified_timestamp desc'
        )
    }}
)
select * from deduped
where not is_deleted
