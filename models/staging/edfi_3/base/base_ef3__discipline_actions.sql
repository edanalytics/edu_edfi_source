with discipline_actions as (
    {{ source_edfi3('discipline_actions') }}
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
        v:disciplineActionIdentifier::string          as discipline_action_id,
        v:disciplineDate::date                        as discipline_date,
        v:assignmentSchoolReference:schoolId::int     as assignment_school_id,
        v:responsibilitySchoolReference:schoolId::int as responsibility_school_id,
        v:studentReference:studentUniqueId::string    as student_unique_id,
        v:disciplineActionLength::float               as discipline_action_length,
        v:actualDisciplineActionLength::float         as actual_discipline_action_length,
        v:iepPlacementMeetingIndicator::boolean       as triggered_iep_placement_meeting,
        v:relatedToZeroTolerancePolicy::boolean       as is_related_to_zero_tolerance_policy,
        --descriptors
        {{ extract_descriptor('v:disciplineActionLengthDifferenceReasonDescriptor::string') }} as discipline_action_length_difference_reason,
        -- references
        v:assignmentSchoolReference     as assignment_school_reference,
        v:responsibilitySchoolReference as responsibility_school_reference,
        v:studentReference              as student_reference,
        -- lists
        v:disciplines                                   as v_disciplines,
        v:studentDisciplineIncidentAssociations         as v_student_discipline_incident_associations,
        v:staffs                                        as v_staffs,
        v:studentDisciplineIncidentBehaviorAssociations as v_student_discipline_incident_behavior_associations,

        -- edfi extensions
        v:_ext as v_ext
    from discipline_actions
)
select * from renamed