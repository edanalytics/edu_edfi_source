with base_stu_programs as (
    select * from {{ ref('base_ef3__student_cte_program_associations') }}
),

keyed as (
    select 
        {{ dbt_utils.generate_surrogate_key(
            [
                'tenant_code',
                'api_year',
                'lower(student_unique_id)',
                'ed_org_id',
                'program_ed_org_id',
                'lower(program_name)',
                'lower(program_type)',
                'program_enroll_begin_date'
            ]
        ) }} as k_student_program,
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
        partition_by='k_student_program',
        order_by='last_modified_timestamp desc, pull_timestamp desc'
    ) }}
)

select * from deduped
where not is_deleted