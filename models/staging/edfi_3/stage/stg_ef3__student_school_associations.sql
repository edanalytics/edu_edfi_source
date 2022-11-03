with base_student_school_assoc as (
    select * from {{ ref('base_ef3__student_school_associations') }}
    where not is_deleted
),
keyed as (
    select 
        {{ gen_skey('k_student') }},
        {{ gen_skey('k_student_xyear') }},
        {{ gen_skey('k_school') }},
        {{ gen_skey('k_school_calendar') }},
        {{ gen_skey('k_graduation_plan') }},
        base_student_school_assoc.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_student_school_assoc
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_student, k_school, entry_date', 
            order_by='pull_timestamp desc'
        )
    }}
)
select * from deduped