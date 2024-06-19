with source_stu_programs as (
    {{ source_edfi3('student_special_education_program_associations') }}
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

        v:id::string as record_guid,
        v:studentReference:studentUniqueId::string                    as student_unique_id,
        v:educationOrganizationReference:educationOrganizationId::int as ed_org_id,
        v:educationOrganizationReference:link:rel::string             as ed_org_type,
        v:programReference:educationOrganizationId::int               as program_ed_org_id,
        v:beginDate::date                                             as program_enroll_begin_date,
        v:endDate::date                                               as program_enroll_end_date,
        v:programReference:programName::string                        as program_name,
        v:servedOutsideOfRegularSession::boolean                      as is_served_outside_regular_session,
        v:participationStatus:designatedBy::string                    as participation_status_designated_by,
        v:participationStatus:statusBeginDate::date                   as participation_status_begin_date,
        v:participationStatus:statusEndDate::date                     as participation_status_end_date,

        v:ideaEligibility::boolean                                    as is_idea_eligible,
        v:iepBeginDate::date                                          as iep_begin_date,
        v:iepEndDate::date                                            as iep_end_date,
        v:iepReviewDate::date                                         as iep_review_date,
        v:lastEvaluationDate::date                                    as last_evaluation_date,
        v:medicallyFragile::boolean                                   as is_medically_fragile,
        v:multiplyDisabled::boolean                                   as is_multiply_disabled,
        v:schoolHoursPerWeek::float                                   as school_hours_per_week,
        v:specialEducationHoursPerWeek::float                         as spec_ed_hours_per_week,

        -- descriptors
        {{ extract_descriptor('v:programReference:programTypeDescriptor::string') }}            as program_type,
        {{ extract_descriptor('v:participationStatus:participationStatusDescriptor::string') }} as participation_status,
        {{ extract_descriptor('v:reasonExitedDescriptor::string') }}                            as reason_exited,
        {{ extract_descriptor('v:specialEducationSettingDescriptor::string') }}                 as special_education_setting,

        -- references
        v:educationOrganizationReference as education_organization_reference,
        v:programReference               as program_reference,
        v:studentReference               as student_reference,

        -- lists
        v:programParticipationStatuses    as v_program_participation_statuses,
        v:disabilities                    as v_disabilities,
        v:serviceProviders                as v_service_providers,
        v:specialEducationProgramServices as v_special_education_program_services,

        -- edfi extensions
        v:_ext                            as v_ext

    from source_stu_programs
)

select * from renamed
