with base_evaluation_ratings as (
    select * from {{ ref('base_tpdm__evaluation_ratings') }}
    where not is_deleted
),
keyed as (
    select
        {{ dbt_utils.generate_surrogate_key(
            [
                'tenant_code',
                'api_year',
                'ed_org_id',
                'evaluation_date',
                'lower(evaluation_period)',
                'lower(evaluation_title)',
                'lower(performance_evaluation_title)',
                'lower(performance_evaluation_type)',
                'lower(person_id)',
                'school_year',
                'lower(source_system)',
                'lower(academic_term)'
            ]
        ) }} as k_evaluation_rating,
        {{ gen_skey('k_course_section') }},
        {{ gen_skey('k_evaluation') }},
        {{ gen_skey('k_performance_evaluation_rating') }},
        base_evaluation_ratings.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_evaluation_ratings
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_evaluation_rating',
            order_by='pull_timestamp desc'
        )
    }}
)
select * from deduped
