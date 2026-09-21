with source_stu_programs as (
    {{ source_edfi3('student_section504_program_associations') }}
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

        v:id::string                                                         as record_guid, 
        ods_version, 
        data_model_version, 
        v:studentReference:studentUniqueId::string                           as student_unique_id, 
        v:educationOrganizationReference:educationOrganizationId::int        as ed_org_id,
        v:educationOrganizationReference:link:rel::string                    as ed_org_type,
        v:beginDate::date                                                    as program_enroll_begin_date,
        v:endDate::date                                                      as program_enroll_end_date, 
        v:programReference:programName::string                               as program_name,
        v:programReference:educationOrganizationId::integer                  as program_ed_org_id,

        v:accommodationPlan::boolean                                         as accommodation_plan, 
        v:section504Eligibility::boolean                                     as section_504_eligibility, 
        v:servedOutsideOfRegularSession::boolean                             as served_outside_of_regular_session,
    
        v:section504EligibilityDecisionDate::date                            as section_504_eligibility_decision_date, 
        v:section504MeetingDate::date                                        as section_504_meeting_date,
        -- descriptors
        {{ extract_descriptor('v:programReference:programTypeDescriptor') }} as program_type,
        {{ extract_descriptor('v:reasonExitedDescriptor') }}                 as reason_exited,
        {{ extract_descriptor('v:section504DisabilityDescriptor') }}         as section_504_disability,

        -- references
        v:educationOrganizationReference                                     as education_organization_reference,
        v:programReference                                                   as program_reference, 
        v:studentReference                                                   as student_reference,

        -- lists
        v:programParticipationStatuses                                       as v_program_participation_statuses,

        -- edfi extensions
        v:_ext                                                               as v_ext
    from source_stu_programs
)

select * from renamed