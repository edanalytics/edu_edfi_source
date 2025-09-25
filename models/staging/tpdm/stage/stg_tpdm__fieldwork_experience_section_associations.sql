with base_student_section_associations as (
    select * from {{ ref('stg_tpdm__fieldwork_experience_section_associations') }}
),
keyed as (
    select 
        {{ gen_skey('k_fieldwork_experience') }},
        {{ gen_skey('k_section') }},  -- should this be k_course_section instead?
        base_student_section_associations.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_student_section_associations
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_fieldwork_experience, k_section',
            order_by='last_modified_timestamp desc, pull_timestamp desc'
        )
    }}
)
select * from deduped
where not is_deleted