with base_evaluation_element_ratings as (
    select * from {{ ref('base_tpdm__evaluation_element_ratings') }}
    where not is_deleted
),
keyed as (
    select
        {{ dbt_utils.surrogate_key(
            [
                'tenant_code',
                'api_year',
                'ed_org_id',
                'evaluation_date',
                'evaluation_element_title',
                'evaluation_objective_title',
                'evaluation_period',
                'evaluation_title',
                'perfomance_evaluation_title',
                'performance_evaluation_type',
                'person_id',
                'school_year',
                'source_system',
                'academic_term'
            ]
        ) }} as k_evaluation_element_rating,
        base_evaluation_element_ratings.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_evaluation_element_ratings
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_evaluation_element_rating',
            order_by='pull_timestamp desc'
        )
    }}
)
select * from deduped
