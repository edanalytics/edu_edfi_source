with source_stu_program as (
    {{ source_edfi3('student_school_food_service_program_associations') }}
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

        v:id::string                                                                as record_guid, 
        ods_version::string,
        data_model_version,
        v:studentReference:studentUniqueId::string                                  as student_unique_id,
        v:educationOrganizationReference:educationOrganizationId::int               as ed_org_id,
        v:educationOrganizationReference:link:rel::string                           as ed_org_type,
        v:programReference:educationOrganizationId::int                             as program_ed_org_id, 
        v:beginDate::date                                                           as begin_date,
        v:endDate::date                                                             as end_date,
        v:programReference:programName::string                                      as program_name,
        v:studentReference:link:rel::string                                         as student_reference_rel, 
        v:programReference:link:rel::string                                         as program_reference_rel,

        v:directCertification::boolean                                              as direct_certification,
        v:servedOutsideOfRegularSession::boolean                                    as served_outside_of_regular_session,

        -- descriptors
        {{ extract_descriptor('v:programReference:programTypeDescriptor') }}        as program_type,
        {{ extract_descriptor('v:reasonExitedDescriptor') }} as reason_exited,

        -- references
        v:educationOrganizationReference                                            as education_organization_reference,
        v:programReference                                                          as program_reference,
        v:studentReference                                                          as student_reference,

        -- lists
        v:programParticipationStatuses                                              as v_program_participation_statuses,
        v:schoolFoodServiceProgramServices                                          as v_school_food_service_program_services,

        -- edfi extensions
        v:_ext                                                                      as v_ext

    from source_stu_program
)

select * from renamed