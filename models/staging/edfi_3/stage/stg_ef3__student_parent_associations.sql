with base_stu_parent as (
    select * from {{ ref('base_ef3__student_parent_associations') }}
),
keyed as (
    select 
        {{ gen_skey('k_student') }},
        {{ gen_skey('k_parent') }},
        api_year as school_year,
        base_stu_parent.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_stu_parent
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_student, k_parent',
            order_by='pull_timestamp desc'
        )
    }}
)
select * from deduped