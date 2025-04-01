with base_performance_evaluations as (
    select * from {{ ref('base_tpdm__performance_evaluations') }}
),
keyed as (
    select
        {{ dbt_utils.generate_surrogate_key(
            ['tenant_code',
            'api_year',
            'ed_org_id',
            'lower(evaluation_period)',
            'lower(performance_evaluation_title)',
            'lower(performance_evaluation_type)',
            'school_year',
            'lower(academic_term)']
        ) }} as k_performance_evaluation,
        {{ edorg_ref(annualize=False) }},
        base_performance_evaluations.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_performance_evaluations
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_performance_evaluation',
            order_by='last_modified_timestamp desc, pull_timestamp desc'
        )
    }}
)
select * from deduped
where not is_deleted
