with student_program_evaluations as (
    select * from {{ source_edfi3('student_program_evaluations') }}
)

, renamed as (
    select

        -- generic
        v:id::string                                                  as record_guid,
        v:educationOrganizationReference:educationOrganizationId::int as ed_org_id,
        v:educationOrganizationReference:link:rel::string             as ed_org_type,
        v:evaluationDate::date                                        as evaluation_date,
        v:evaluationDuration::int                                     as evaluation_duration,
        v:studentReference:studentUniqueId::string                    as student_unique_id,
        v:summaryEvaluationComment::string                            as summary_evaluation_comment,
        v:summaryEvaluationNumericRating::float                       as summary_evaluation_numeric_rating,

        -- references
        v:educationOrganizationReference as education_organization_reference,
        v:programEvaluationReference     as program_evaluation_reference,
        v:staffEvaluatorStaffReference   as staff_evaluator_staff_reference,
        v:studentReference               as student_reference,

        -- lists
        v:externalEvaluators          as v_external_evaluators,
        v:studentEvaluationElements   as v_student_evaluation_elements,
        v:studentEvaluationObjectives as v_student_evaluation_objectives,

        -- descriptors
        {{ extract_descriptor('v:summaryEvaluationRatingLevelDescriptor::string') }} as summary_evaluation_rating_level,

        -- ed-fi extensions
        v:_ext as v_ext,

    from student_program_evaluations
)

select * from renamed