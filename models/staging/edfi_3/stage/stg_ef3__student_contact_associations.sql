with base_stu_contact as (
    select * from {{ ref('base_ef3__student_contact_associations') }}
    where not is_deleted
),
base_stu_parent as (
    select * rename parent_reference as contact_reference
    from {{ ref('base_ef3__student_parent_associations') }}
    where not is_deleted
),
-- parents were renamed to contacts in Data Standard v5.0
unioned as (
    select * from base_stu_contact
    union 
    select * from base_stu_parent
),
keyed as (
    select 
        {{ gen_skey('k_student') }},
        {{ gen_skey('k_contact') }},
        {{ gen_skey('k_student_xyear') }},
        api_year as school_year,
        unioned.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from unioned
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_student, k_contact',
            order_by='pull_timestamp desc'
        )
    }}
)
select * from deduped