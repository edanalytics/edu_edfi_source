with source_stu_programs as (
    {{ source_edfi3('student_cte_program_associations') }}
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

        v:id::string                                                                    as record_guid, 
        ods_version, 
        data_model_version,
        v:studentReference:studentUniqueId::int                                         as student_unique_id,
        v:educationOrganizationReference:educationOrganizationId::int                   as ed_org_id,
        v:educationOrganizationReference:link:rel::string                               as ed_org_type,
        v:programReference:educationOrganizationId::int                                 as program_ed_org_id,
        v:beginDate::date                                                               as program_enroll_begin_date, 
        v:endDate::date                                                                 as program_enroll_end_date, 
        v:programReference:programName::string                                          as program_name,

        v:nonTraditionalGenderStatus::boolean                                           as non_traditional_gender_status,
        v:privateCTEProgram::boolean                                                    as private_cte_program,
        v:ServedOutsideOfRegularSession::boolean                                        as served_outside_of_regular_session,

        -- descriptors
        {{ extract_descriptor('v:technicalSkillsAssessmentDescriptor') }}               as technical_skills_assessment,
        {{ extract_descriptor('v:programReference:programTypeDescriptor') }}            as program_type,
        {{ extract_descriptor('v:ReasonExitedDescriptor:ReasonExitedDescriptorId') }}   as reason_exited,

        -- references
        v:educationOrganizationReference                                                as education_organization_reference,
        v:programReference                                                              as program_reference, 
        v:studentReference                                                              as student_reference,

        -- lists
        v:cteProgramServices                                                            as v_cte_program_services, 
        v:ctePrograms                                                                   as v_cte_programs, 
        v:programParticipationStatuses                                                  as v_program_participation_statuses, 
        v:services                                                                      as v_services,

        -- edfi extensions
        v:_ext as v_ext
        
    from source_stu_programs
)

select * from renamed