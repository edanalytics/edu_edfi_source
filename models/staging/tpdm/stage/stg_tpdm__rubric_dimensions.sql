with base_rubric_dimensions as (
    select * from {{ ref('base_tpdm__rubric_dimensions') }}
),
keyed as (
    select
        {{ dbt_utils.generate_surrogate_key(
            [
                'tenant_code',
                'api_year',
                'ed_org_id',
                'lower(evaluation_element_title)',
                'lower(evaluation_objective_title)',
                'lower(evaluation_period)',
                'lower(evaluation_title)',
                'lower(performance_evaluation_title)',
                'lower(performance_evaluation_type)',
                'rubric_rating',
                'school_year',
                'lower(academic_term)'
            ]
        ) }} as k_rubric_dimension,
        {{ gen_skey('k_evaluation_element') }},
        base_rubric_dimensions.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_rubric_dimensions
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_rubric_dimension',
            order_by='last_modified_timestamp desc, pull_timestamp desc'
        )
    }}
)
select * from deduped
where not is_deleted
