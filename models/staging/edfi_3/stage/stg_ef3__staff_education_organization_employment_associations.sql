with base_staff_ed_org_employ as (
    select * from {{ ref('base_ef3__staff_education_organization_employment_associations') }}
),
keyed as (
    select 
        {{ gen_skey('k_staff') }},
        {{ edorg_ref() }},
        api_year as school_year,
        base_staff_ed_org_employ.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_staff_ed_org_employ
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by= 'tenant_code, api_year, ed_org_id, employment_status, hire_date, staff_unique_id',
            order_by='api_year desc, last_modified_timestamp desc, pull_timestamp desc'
        )
    }}
)
select * from deduped
where not is_deleted
