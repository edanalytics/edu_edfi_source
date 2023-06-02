with source_stu_programs as (
    {{ source_edfi3('student_language_instruction_program_associations') }}
),

renamed as (
    select
        -- generic columns
        tenant_code,
        api_year,
        pull_timestamp,
        file_row_number,
        filename,
        is_deleted,

        v:id::string                                                      as record_guid,
        v:studentReference:studentUniqueId::string                        as student_unique_id,
        v:educationOrganizationReference:educationOrganizationId::integer as ed_org_id,
        v:educationOrganizationReference:link:rel::string                 as ed_org_type,
        v:programReference:educationOrganizationId::integer               as program_ed_org_id,
        v:beginDate::date                                                 as program_enroll_begin_date,
        v:endDate::date                                                   as program_enroll_end_date,
        v:programReference:programName::string                            as program_name,
        v:servedOutsideOfRegularSession::boolean                          as is_served_outside_regular_session,
        v:participationStatus:designatedBy::string                        as participation_status_designated_by,
        v:participationStatus:statusBeginDate::date                       as participation_status_begin_date,
        v:participationStatus:statusEndDate::date                         as participation_status_end_date,

        v:englishLearnerParticipation::boolean                            as has_english_learner_participation,
        v:dosage::integer                                                 as intervention_duration_min,

        -- descriptors
        {{ extract_descriptor('v:participationStatus:participationStatusDescriptor::string') }} as participation_status,
        {{ extract_descriptor('v:programReference:programTypeDescriptor::string') }}            as program_type,
        {{ extract_descriptor('v:reasonExitedDescriptor::string') }}                            as reason_exited,

        -- references
        v:educationOrganizationReference as education_organization_reference,
        v:programReference               as program_reference,
        v:studentReference               as student_reference,

        -- lists
        v:programParticipationStatuses          as v_program_participation_statuses,
        v:languageInstructionProgramServices    as v_language_instruction_program_services,
        v:englishLanguageProficiencyAssessments as v_english_language_proficiency_assessments,

        -- edfi extensions
        v:_ext as v_ext

    from source_stu_programs
)

select * from renamed
