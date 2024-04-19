with student_cte_program_associations as (
     {{  edu_edfi_source.source_edfi3('student_cte_program_associations')  }} 
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
        v:id::string as record_guid
        
        , v:beginDate::date as program_enroll_begin_date
        , v:endDate::date as program_enroll_end_date
        , v:educationOrganizationReference as education_organization_reference
        , v:educationOrganizationReference:educationOrganizationId::int as ed_org_id
        , v:programReference as program_reference
        , v:programReference:educationOrganizationId::int as program_ed_org_id
        , v:programReference:programName::string as program_name
        ,  {{  edu_edfi_source.extract_descriptor('v:programReference:programTypeDescriptor::string')  }}  as program_type
        , v:studentReference as student_reference
        , v:studentReference:studentUniqueId::string as student_unique_id
        , v:ctePrograms::array as v_cte_programs
        , v:cteProgramServices::array as v_cte_program_services
        , v:nonTraditionalGenderStatus::boolean as non_traditional_gender_status
        , v:participationStatus::string as participation_status
        , v:privateCTEProgram::boolean as private_cte_program
        , v:programParticipationStatuses::array as v_program_participation_statuses
        ,  {{  edu_edfi_source.extract_descriptor('v:reasonExitedDescriptor::string')  }}  as reason_exited
        , v:servedOutsideOfRegularSession::boolean as served_outside_of_regular_session
        , v:services::array as v_services
        ,  {{  edu_edfi_source.extract_descriptor('v:technicalSkillsAssessmentDescriptor::string')  }}  as technical_skills_assessment
        , v:_ext as v_ext
    from student_cte_program_associations
)
select * from renamed
