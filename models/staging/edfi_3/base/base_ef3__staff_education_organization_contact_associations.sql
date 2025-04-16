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
        v:educationOrganizationReference:educationOrganizationId::int as ed_org_id,
        v:electronicMailAddress::string                               as email_address,
        v:staffReference:staffUniqueId::int                           as staff_unique_id,
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
