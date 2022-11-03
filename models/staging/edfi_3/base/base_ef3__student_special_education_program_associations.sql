with student_spec_ed as (
    {{ source_edfi3('student_special_education_program_associations') }}
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
        -- ref columns
        v:educationOrganizationReference:educationOrganizationId::int as ed_org_id,
        v:educationOrganizationReference:link:rel::string             as ed_org_type,
        v:programReference:educationOrganizationId::int               as program_ed_org_id,
        v:programReference:programName::string                        as program_name,
        v:studentReference:studentUniqueId::int                       as student_unique_id,
        -- columns
        v:beginDate::date                           as spec_ed_program_begin_date,
        v:endDate::date                             as spec_ed_program_end_date,
        v:ideaEligibility::boolean                  as is_idea_eligible,
        v:iepBeginDate::date                        as iep_begin_date,
        v:iepEndDate::date                          as iep_end_date,
        v:iepReviewDate::date                       as iep_review_date,
        v:lastEvaluationDate::date                  as last_evaluation_date,
        v:medicallyFragile::boolean                 as is_medically_fragile,
        v:multiplyDisabled::boolean                 as is_multiply_disabled,
        v:schoolHoursPerWeek::float                 as school_hours_per_week,
        v:specialEducationHoursPerWeek::float       as spec_ed_hours_per_week,
        v:servedOutsideOfRegularSession::boolean    as is_served_outside_regular_session,
        v:participationStatus:designatedBy::string  as participation_status_designated_by,
        v:participationStatus:statusBeginDate::date as participation_status_begin_date,
        v:participationStatus:statusEndDate::date   as participation_status_end_date,
        -- descriptors
        {{ extract_descriptor('v:programReference:programTypeDescriptor::string') }}            as program_type,
        {{ extract_descriptor('v:participationStatus:participationStatusDescriptor::string') }} as participation_status,
        {{ extract_descriptor('v:reasonExitedDescriptor::string') }}                            as reason_exited,
        {{ extract_descriptor('v:specialEducationSettingDescriptor::string') }}                 as special_education_setting,
        -- unflattened lists
        v:disabilities                    as v_disabilities,
        v:programParticipationStatuses    as v_program_participation_statuses,
        v:serviceProviders                as v_service_providers,  
        v:specialEducationProgramServices as v_special_education_program_services,
        -- references
        v:educationOrganizationReference as education_organization_reference,
        v:programReference               as program_reference,
        v:studentReference               as student_reference,

        v:_ext                            as v_ext
    from student_spec_ed
)
select * from renamed