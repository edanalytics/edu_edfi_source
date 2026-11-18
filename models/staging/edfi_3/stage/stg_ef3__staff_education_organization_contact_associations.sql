-- SPDX-FileCopyrightText: 2022 Education Analytics
-- SPDX-License-Identifier: MPL-2.0

with base_staff_ed_org_contact_assoc as (
    select * from {{ ref('base_ef3__staff_education_organization_contact_associations') }}
),
keyed as (
    select 
        {{ gen_skey('k_staff') }},
        {{ edorg_ref() }},
        {{ dbt_utils.generate_surrogate_key(
            [
                'tenant_code',
                'api_year',
                'contact_title',
                'ed_org_id',
                'staff_unique_id'
            ]
        ) }} as k_staff_ed_org_contact,
        api_year as school_year,
        base_staff_ed_org_contact_assoc.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_staff_ed_org_contact_assoc
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by= 'k_staff_ed_org_contact',
            order_by='last_modified_timestamp desc, pull_timestamp desc'
        )
    }}
)
select * from deduped
where not is_deleted