with base_rubric_dimensions as (
    select * from {{ ref('base_tpdm__rubric_dimensions') }}
    where not is_deleted
),
keyed as (
    select
        {{ dbt_utils.surrogate_key(
            [
                'tenant_code',
                'api_year',
                'ed_org_id',
                'lower(evaluation_element_title)',
                'lower(evaluation_objective_title)',
                'lower(evaluation_period)',
                'lower(valuation_title)',
                'lower(performance_evaluation_title)',
                'lower(performance_evaluation_type)',
                'school_year',
                'rubric_rating',
                'lower(academic_term)'
            ]
        ) }} as k_rubric_dimension,
        base_rubric_dimensions.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_rubric_dimensions
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_rubric_dimension',
            order_by='pull_timestamp desc'
        )
    }}
)
select * from deduped
