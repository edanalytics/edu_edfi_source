with staff_ed_org_employ as (
    {{ source_edfi3('staff_education_organization_employment_associations') }}
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
        v:id::string as record_guid,
        v:educationOrganizationReference:educationOrganizationId::int as ed_org_id,
        v:educationOrganizationReference:link:rel::string             as ed_org_type,
        v:staffReference:staffUniqueId::string                        as staff_unique_id,
        v:credentialReference:credentialIdentifier::string            as credential_identifier,
        v:department::string                                          as department,
        v:hireDate::date                                              as hire_date,
        v:endDate::date                                               as end_date,
        v:fullTimeEquivalency::float                                  as full_time_equivalency,
        v:hourlyWage::float                                           as hourly_wage,
        v:annualWage::float                                           as annual_wage,
        v:offerDate::date                                             as offer_date,
        -- descriptors
        {{ extract_descriptor('v:employmentStatusDescriptor::string') }}                                  as employment_status,
        {{ extract_descriptor('v:credentialReference:stateOfIssueStateAbbreviationDescriptor::string') }} as credential_state,
        {{ extract_descriptor('v:separationDescriptor::string') }}                                        as separation,
        {{ extract_descriptor('v:separationReasonDescriptor::string') }}                                  as separation_reason,
        -- references
        v:credentialReference            as credential_reference,
        v:educationOrganizationReference as education_organization_reference,
        v:staffReference                 as staff_reference,
        -- edfi extensions
        v:_ext as v_ext
    from staff_ed_org_employ
)
select * from renamed