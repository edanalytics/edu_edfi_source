with base_grades as (
    select * from {{ ref('base_ef3__grades') }}
    where not is_deleted
),
keyed as (
    select
        {{ gen_skey('k_student', 'student_section_association_reference') }},
        {{ gen_skey('k_school', 'student_section_association_reference') }},
        {{ gen_skey('k_grading_period') }},
        {{ gen_skey('k_course_section', 'student_section_association_reference') }},
        base_grades.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_grades
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_grading_period, k_student, k_school, k_course_section, grade_type',
            order_by='pull_timestamp desc'
        )
    }}
)
select * from deduped
order by tenant_code, school_year desc, student_unique_id