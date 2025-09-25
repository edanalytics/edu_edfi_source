with base_quantitative_measures as (
    select * from {{ ref('base_tpdm__quantitative_measures') }}
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
                'lower(quantitative_measure_identifier)',
                'school_year',
                'lower(academic_term)'
            ]
        ) }} as k_quantitative_measure,
        {{ gen_skey('k_evaluation_element') }},
        base_quantitative_measures.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_quantitative_measures
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_quantitative_measure',
            order_by='last_modified_timestamp desc, pull_timestamp desc'
        )
    }}
)
select * from deduped
where not is_deleted