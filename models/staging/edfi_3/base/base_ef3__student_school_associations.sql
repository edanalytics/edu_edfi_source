with stu_school_assoc as (
    {{ source_edfi3('student_school_associations') }}
),
renamed as (
    select
        tenant_code,
        api_year,
        pull_timestamp,
        file_row_number,
        filename,
        is_deleted,
        v:id::string                                  as record_guid,
        v:schoolReference:schoolId::integer           as school_id,
        v:studentReference:studentUniqueId::string    as student_unique_id,
        v:schoolYearTypeReference:schoolYear::integer as school_year,
        v:entryDate::date                             as entry_date,
        v:exitWithdrawDate::date                      as exit_withdraw_date,
        v:primarySchool::boolean                      as is_primary_school,
        v:repeatGradeIndicator::boolean               as is_repeat_grade,
        v:schoolChoiceTransfer::boolean               as is_school_choice_transfer,
        v:employedWhileEnrolled::boolean              as is_employed_while_enrolled,
        v:fullTimeEquivalency::float                  as full_time_equivalency,
        v:termCompletionIndicator                     as completed_term,
        v:calendarReference:calendarCode::string      as calendar_code,
        v:classOfSchoolYearTypeReference:schoolYear   as class_of_school_year,
        -- descriptors
        {{ extract_descriptor('v:residencyStatusDescriptor::string') }}       as residency_status,
        {{ extract_descriptor('v:entryGradeLevelDescriptor::string') }}       as entry_grade_level,
        {{ extract_descriptor('v:entryGradeLevelReasonDescriptor::string') }} as entry_grade_level_reason,
        {{ extract_descriptor('v:entryTypeDescriptor::string') }}             as entry_type,
        {{ extract_descriptor('v:exitWithdrawTypeDescriptor::string') }}      as exit_withdraw_type,
        {{ extract_descriptor('v:graduationPlanReference:graduationPlanTypeDescriptor::string') }} as graduation_plan_type,
        -- references
        v:calendarReference       as calendar_reference,
        v:schoolReference         as school_reference,
        v:studentReference        as student_reference,
        v:graduationPlanReference as graduation_plan_reference,
        -- lists
        v:alternativeGraduationPlans as v_alternative_graduation_plans,
        v:educationPlans             as v_education_plans,

        -- edfi extensions
        v:_ext as v_ext
    from stu_school_assoc
)
select * from renamed
