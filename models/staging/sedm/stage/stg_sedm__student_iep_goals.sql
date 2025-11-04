with base_student_iep_goals as (
    select * from {{ ref('base_sedm__student_iep_goals') }}
),

keyed as (
    select 
        {{ dbt_utils.generate_surrogate_key(
            [
                'tenant_code',
                'api_year',
                'lower(iep_goal_id)',
                'lower(student_unique_id)',
                'lower(student_iep_association_id)'
            ]
        ) }} as k_student_iep_goal,
        {{ gen_skey('k_student') }},
        {{ gen_skey('k_student_xyear') }},
        {{ gen_skey('k_student_iep_association') }},
        api_year as school_year,
        base_student_iep_goals.*
        {{ extract_extension(model_name=this.name, flatten=True) }}

    from base_student_iep_goals
),

deduped as (
    {{ dbt_utils.deduplicate(
        relation='keyed',
        partition_by='k_student_iep_goal',
        order_by='last_modified_timestamp desc, pull_timestamp desc'
    ) }}
)

select * from deduped
where not is_deleted
