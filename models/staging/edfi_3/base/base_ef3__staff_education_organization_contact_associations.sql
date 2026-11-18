-- SPDX-FileCopyrightText: 2022 Education Analytics
-- SPDX-License-Identifier: MPL-2.0

with staff_ed_org_contact_assoc as (
    {{ source_edfi3('staff_education_organization_contact_associations') }}
),
renamed as (
    select
        tenant_code,
        api_year,
        pull_timestamp,
        last_modified_timestamp,
        file_row_number,
        filename,
        is_deleted,
        v:id::string                                                  as record_guid,
        v:contactTitle::string                                        as contact_title,
        {{ extract_descriptor('v:contactTypeDescriptor::string') }}   as contact_type,
        v:educationOrganizationReference:educationOrganizationId::int as ed_org_id,
        lower(v:electronicMailAddress::string)                        as email_address,
        v:staffReference:staffUniqueId::string                        as staff_unique_id,
        -- make this into an array so that it can be flattened like all other
        -- address and get the benefit of that flatten arrays macro
        {{ edu_edfi_source.json_array_construct('v:address') }}       as v_addresses,
        -- arrays
        v:telephones                                                  as v_telephones,
        -- references
        v:educationOrganizationReference                              as education_organization_reference,
        v:staffReference                                              as staff_reference,
        -- edfi extensions
        v:_ext                                                        as v_ext

    from staff_ed_org_contact_assoc
)
select * from renamed
