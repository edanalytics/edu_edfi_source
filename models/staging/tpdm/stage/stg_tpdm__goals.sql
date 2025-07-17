with base_goals as (
    select * from {{ ref('base_tpdm__goals') }}
),
keyed as (
    select
        {{ dbt_utils.generate_surrogate_key(
            [
                'tenant_code',
                'api_year',
                'assignment_date',
                'lower(goal_title)',
                'lower(person_id)',
                'lower(person_source_system)'
            ]
        ) }} as k_goal,
        {{ gen_skey('k_person') }},
        {{ gen_skey('k_evaluation_element') }},
        {{ gen_skey('k_evaluation_objective') }},
        {{ gen_skey('k_goal', alt_ref='parent_goal_reference', alt_k_name='k_goal__parent') }}, 
        base_goals.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_goals
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_goal',
            order_by='last_modified_timestamp desc, pull_timestamp desc'
        )
    }}
)
select * from deduped
where not is_deleted