with source_stu_programs as (
    {{ source_edfi3('student_migrant_education_program_associations') }}
),

renamed as (
    select
        -- generic columns
        tenant_code,
        api_year,
        pull_timestamp,
        last_modified_timestamp,
        file_row_number,
        filename,
        is_deleted,

        v:id::string                                                                                as record_guid, 
        ods_version, 
        data_model_version, 
        v:studentReference:studentUniqueId::string                                                  as student_unique_id, 
        v:educationOrganizationReference:educationOrganizationId::int                               as ed_org_id,
        v:educationOrganizationReference:link:rel::string                                           as ed_org_type,
        v:beginDate::date                                                                           as program_enroll_begin_date,
        v:endDate::date                                                                             as program_enroll_end_date, 
        v:programReference:programName::string                                                      as program_name, 
        v:programReference:educationOrganizationId::int                                             as program_ed_org_id,

        v:priorityForServices::boolean                                                              as priority_for_service, 

        v:lastQualifyingMove::date                                                                  as last_qualifying_move, 
        v:usInitialEntry::date                                                                      as us_initial_entry,
        v:usMostRecentEntry::date                                                                   as us_most_recent_entry,
        v:USInitialSchoolEntry::date                                                                as us_initial_school_entry,
        v:qualifyingArrivalDate::date                                                               as qualifying_arrival_date,
        v:stateResidencyDate::date                                                                  as state_residency_date,
        v:eligibilityExpirationDate::date                                                           as eligibility_expiration_date,
        -- descriptors
        {{ extract_descriptor('v:programReference:programTypeDescriptor') }}                        as program_type,
        {{ extract_descriptor('v:continuationOfServicesReasonDescriptor') }}                        as continuation_of_services_reason,

        -- references
        v:educationOrganizationReference                                                            as education_organization_reference,
        v:programReference                                                                          as program_reference, 
        v:studentReference                                                                          as student_reference,

        -- lists
        v:migrantEducationProgramServices                                                           as v_migrant_education_program_services,
        v:programParticipationStatuses                                                              as v_program_participation_statuses,

        -- edfi extensions
        v:_ext                                                                                      as v_ext
    from source_stu_programs
)

select * from renamed

