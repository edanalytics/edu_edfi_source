with staff_ed_org_assign as (
    {{ source_edfi3('staff_education_organization_assignment_associations') }}
),
renamed as (
    select 
        tenant_code,
        api_year,
        pull_timestamp,
        file_row_number,
        filename,
        is_deleted,
        v:id::string as record_guid,
        v:educationOrganizationReference:educationOrganizationId::int as ed_org_id,
        v:educationOrganizationReference:link:rel::string             as ed_org_type,
        v:staffReference:staffUniqueId::string                        as staff_unique_id,
        v:positionTitle::string                                       as position_title,
        v:beginDate::date                                             as begin_date,
        v:endDate::date                                               as end_date,
        v:fullTimeEquivalency::float                                  as full_time_equivalency,
        v:orderOfAssignment::float                                    as order_of_assignment,
        v:credentialReference:credentialIdentifier::string            as credential_identifier,
        -- descriptors
        {{ extract_descriptor('v:credentialReference:stateOfIssueStateAbbreviationDescriptor::string') }} as credential_state,
        {{ extract_descriptor('v:staffClassificationDescriptor::string') }}                               as staff_classification,
        -- references
        v:credentialReference            as credential_reference,
        v:educationOrganizationReference as education_organization_reference,
        v:staffReference                 as staff_reference,
        v:employmentStaffEducationOrganizationEmploymentAssociationReference as staff_ed_org_employment_reference,

        -- edfi extensions
        v:_ext as v_ext
    from staff_ed_org_assign
)
select * from renamed