with base_student_section as (
    select * from {{ ref('base_ef3__student_section_associations') }}
),
keyed as (
    select 
        {{ gen_skey('k_student') }},
        {{ gen_skey('k_student_xyear') }},
        {{ gen_skey('k_course_section') }},
        base_student_section.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_student_section
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_student, k_course_section, begin_date',
            order_by='last_modified_timestamp desc, pull_timestamp desc'
        )
    }}
)
select * from deduped
where not is_deleted
