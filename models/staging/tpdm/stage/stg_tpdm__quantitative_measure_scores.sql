with base_quantitative_measure_scores as (
    select * from {{ ref('base_tpdm__quantitative_measure_scores') }}
),
keyed as (
    select
        {{ gen_skey('k_quantitative_measure') }},
        {{ gen_skey('k_evaluation_element_rating') }},
        base_quantitative_measure_scores.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_quantitative_measure_scores
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_evaluation_element_rating, k_quantitative_measure',
            order_by='last_modified_timestamp desc, pull_timestamp desc'
        )
    }}
)
select * from deduped
where not is_deleted