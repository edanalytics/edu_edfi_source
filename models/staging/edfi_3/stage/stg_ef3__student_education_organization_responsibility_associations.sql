with base_stu_responsibility as (
    select * from {{ ref('base_ef3__student_education_organization_responsibility_associations') }}
    where not is_deleted
),
keyed as (
    select 
        {{ gen_skey('k_student') }},
        {{ gen_skey('k_student_xyear') }},
        {{ edorg_ref() }},
        api_year as school_year,
        base_stu_responsibility.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_stu_responsibility
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_student, begin_date, school_year',
            order_by='pull_timestamp desc')
    }}
)
select * from deduped