with student_special_education_program_eligibility_associations as (
  select * from {{ source("student_special_education_program_eligibility_associations") }}
),

renamed as (
    select
        tenant_code,
        api_year,
        pull_timestamp,
        file_row_number,
        last_modified_timestamp,
        filename,
        is_deleted,
        ods_version,
        data_model_version,

        v:id::string                            as record_guid,
        v:consentToEvaluationDate::date         as consent_to_evaluation_date,
        v:consentToEvaluationReceivedDate::date as consent_to_evaluation_received_date,
        v:eligibilityConferenceDate::date       as eligibility_conference_date
        v:eligibilityDeterminationDate::date    as eligibility_determination_date
        v:eligibilityEvaluationDate::date       as eligibility_evaluation_date,
        v:evaluationCompleteIndicator::boolean  as is_evaluation_complete,
        v:evaluationDelayDays::int              as evaluation_delay_days,
        v:evaluationLateReason::string          as evaluation_late_reason,
        v:ideaIndicator::boolean                as is_idea
        v:originalECIServicesDate::date         as original_eci_services_date,
        v:transitionConferenceDate::date        as transition_conference_date
        v:transitionNotificationDate::date      as transition_notification_date

        -- descriptors
        {{ extract_descriptor('v:eligibilityDelayReasonDescriptor::string') }}    as eligibility_delay_reason
        {{ extract_descriptor('v:eligibilityEvaluationTypeDescriptor::string') }} as eligibility_evaluation_type
        {{ extract_descriptor('v:evaluationDelayReasonDescriptor::string') }}     as evaluation_delay_reason
        {{ extract_descriptor('v:ideaPartDescriptor::string') }}                  as idea_part

        -- references
        v:studentReference as student_reference
        v:programReference as program_reference
        v:educationOrganizationReference as education_organization_reference

        -- edfi extensions
        v:_ext as v_ext

    from student_special_education_program_eligibility_associations
)

select * from renamed