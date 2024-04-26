with base_student_cte_program_associations as (
    select *
    from  {{  ref('base_ef3__student_cte_program_associations')  }} 
    where not is_deleted
),
keyed as (
    select
        {{ edu_edfi_source.gen_skey('k_student') }},
        {{ edu_edfi_source.gen_skey('k_student_xyear') }},
        {{ edu_edfi_source.gen_skey('k_program') }},
        {{ edorg_ref(annualize=False) }},
        api_year as school_year,
        *
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_student_cte_program_associations
),
deduped as (
     {{ 
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_student, k_program, begin_date',
            order_by='pull_timestamp desc'
        )
     }} 
)
select * from deduped