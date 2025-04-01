with base_staff_section_assoc as (
    select * from {{ ref('base_ef3__staff_section_associations') }}
),
keyed as (
    select 
        {{ gen_skey('k_staff') }},
        {{ gen_skey('k_course_section') }},
        base_staff_section_assoc.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_staff_section_assoc
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_staff, k_course_section',
            order_by='last_modified_timestamp desc, pull_timestamp desc'
        )
    }}
)
select * from deduped
where not is_deleted
