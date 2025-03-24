with base_stu_ed_org as (
    select * from {{ ref('base_ef3__student_education_organization_associations') }}
),
keyed as (
    select 
        {{ gen_skey('k_student') }},
        {{ gen_skey('k_student_xyear') }},
        {{ edorg_ref(annualize=False) }},
        api_year as school_year,
        base_stu_ed_org.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_stu_ed_org
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_student, ed_org_id',
            order_by='last_modified_timestamp desc'
        )
    }}
)
select * from deduped
where not is_deleted
