with base_stu_cohorts_assoc as (
    select * from {{ ref('base_ef3__student_cohort_associations') }}
    where not is_deleted
),
keyed as (
    select 
        {{ gen_skey('k_student') }},
        {{ gen_skey('k_student_xyear') }},
        {{ gen_skey('k_cohort')}},
        api_year as school_year,
        base_stu_cohorts_assoc.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_stu_cohorts_assoc
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_student, k_cohort, cohort_begin_date, cohort_ed_org_id, school_year',
            order_by='pull_timestamp desc')
    }}
)
select * from deduped