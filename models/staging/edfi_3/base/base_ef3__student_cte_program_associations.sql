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

        ods_version, 
        data_model_version,
        v:id::string                                                            as record_guid, 
        TRY_TO_DATE(TRIM(v:beginDate::string), 'YYYY-MM-DD')                    as begin_date, 
        TRY_TO_DATE(TRIM(v:endDate::string), 'YYYY-MM-DD')                      as end_date, 

        v:nonTraditionalGenderStatus::boolean                                   as non_traditional_gender_status,
        v:privateCTEProgram::boolean                                            as private_cte_program,
        v:educationOrganizationReference:educationOrganizationId::int           as ed_org_id,
        v:educationOrganizationReference:link:rel::string                       as ed_org_type,
        v:programReference:educationOrganizationId::int                         as program_ed_org_id,
        v:programReference:programName::string                                  as program_name,
        v:programReference:link:rel::string                                     as program_reference_rel,
        v:studentReference:studentUniqueId::int                                 as student_unique_id,
        v:studentReference:link:rel::string                                     as student_reference_rel,

        -- descriptors
        {{ extract_descriptor('v:technicalSkillsAssessmentDescriptor') }}       as technical_skills_assessment_descriptor,
        {{ extract_descriptor('v:programReference:programTypeDescriptor') }}    as program_type_descriptor,

        -- references
        v:educationOrganizationReference                                        as education_organization_reference,
        v:programReference                                                      as program_reference, 
        v:studentReference                                                      as student_reference,

        -- lists
        v:cteProgramServices                                                    as cte_program_services, 
        v:ctePrograms                                                           as cte_programs, 
        v:programParticipationStatuses                                          as program_participation_statuses, 
        v:services                                                              as services
        
    from source_stu_programs
)

select * from renamed