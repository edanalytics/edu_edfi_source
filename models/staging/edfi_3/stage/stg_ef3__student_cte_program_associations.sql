with base_stu_programs as (
    select * from {{ ref('base_ef3__student_cte_program_associations') }}
),

keyed as (
    select 
        {{ gen_skey('k_student') }},
        {{ gen_skey('k_student_xyear') }},
        {{ gen_skey('k_program') }},
        {{ edorg_ref(annualize=False) }},
        api_year as school_year,
        base_stu_programs.*
        {{ extract_extension(model_name=this.name, flatten=True) }}

    from base_stu_programs
),

deduped as (
    {{ dbt_utils.deduplicate(
        relation='keyed',
        partition_by='k_student, k_program, program_enroll_begin_date, school_year',
        order_by='last_modified_timestamp desc, pull_timestamp desc'
    ) }}
)

select * from deduped
where not is_deleted