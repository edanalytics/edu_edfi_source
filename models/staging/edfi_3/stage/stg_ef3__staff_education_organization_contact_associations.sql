with base_staff_ed_org_contact_assoc as (
    select * from {{ ref('base_ef3__staff_education_organization_contact_associations') }}
    where not is_deleted
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
            partition_by= 'tenant_code, api_year, ed_org_id, staff_unique_id, email_address',
            order_by='api_year desc, pull_timestamp desc'
        )
    }}
)
select * from deduped