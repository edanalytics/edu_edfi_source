with base_staff_ed_org_contact_assoc as (
    select * from {{ ref('base_ef3__staff_education_organization_contact_associations') }}
),
keyed as (
    select 
        {{ gen_skey('k_staff') }},
        {{ edorg_ref() }},
        api_year as school_year,
        base_staff_ed_org_contact_assoc.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_staff_ed_org_contact_assoc
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by= 'k_staff, ed_org_id, contact_title, api_year',
            order_by='last_modified_timestamp desc, pull_timestamp desc'
        )
    }}
)
select * from deduped
where not is_deleted