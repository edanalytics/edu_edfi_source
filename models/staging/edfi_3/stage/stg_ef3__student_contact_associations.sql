with base_stu_contact as (
    select 
        *,
        contact_reference:contactUniqueId as contact_unique_id
    from {{ ref('base_ef3__student_contact_associations') }}
),
base_stu_parent as (
    select 
        *,
        parent_reference:parentUniqueId as contact_unique_id --rename to support union and key generation
    from {{ ref('base_ef3__student_parent_associations') }}
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
        -- we can't use the gen_skey macro here because we're bringing in the deprecated parents endpoint data, which contains a parentReference that won't work
        {{ dbt_utils.generate_surrogate_key(['tenant_code', 'lower(contact_unique_id)']) }} as k_contact,
        {{ gen_skey('k_student_xyear') }},
        api_year as school_year,
        unioned.*
        {{ extract_extension(model_name=[this.name, 'stg_ef3__student_parent_associations'], flatten=True) }}
    from unioned
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_student, k_contact',
            order_by='last_modified_timestamp desc'
        )
    }}
)
select * from deduped
where not is_deleted
