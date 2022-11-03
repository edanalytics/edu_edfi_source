with base_stu_spec_ed as (
    select * from {{ ref('base_ef3__student_special_education_program_associations') }}
    where not is_deleted
),
keyed as (
    select 
        {{ gen_skey('k_student') }}, 
        {{ gen_skey('k_student_xyear') }}, 
        {{ gen_skey('k_program') }},
        {{ edorg_ref(annualize=False) }},
        base_stu_spec_ed.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_stu_spec_ed
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_student, ed_org_id, k_program, spec_ed_program_begin_date',
            order_by='pull_timestamp desc'
        )
    }}
)

select * from deduped