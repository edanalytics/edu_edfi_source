with student_program_evaluations as (
    {{ source_edfi3('student_program_evaluations') }}
),

renamed as (
    select

        -- generic
        tenant_code,
        api_year,
        pull_timestamp,
        last_modified_timestamp,
        file_row_number,
        filename,
        is_deleted,
        ods_version,
        data_model_version,

        v:id::string                                                     as record_guid,
        v:educationOrganizationReference:educationOrganizationId::int    as ed_org_id,
        v:educationOrganizationReference:link:rel::string                as ed_org_type,
        v:evaluationDate::date                                           as evaluation_date,
        v:evaluationDuration::int                                        as evaluation_duration,
        v:programEvaluationReference:programEducationOrganizationId::int as program_evaluation_ed_org_id,
        v:programEvaluationReference:programEvaluationTitle::string      as program_evaluation_title,
        v:programEvaluationReference:programName::string                 as program_name,
        v:staffEvaluatorStaffReference:staffUniqueId::string             as staff_unique_id,
        v:studentReference:studentUniqueId::string                       as student_unique_id,
        v:summaryEvaluationComment::string                               as summary_evaluation_comment,
        v:summaryEvaluationNumericRating::float                          as summary_evaluation_numeric_rating,

        -- descriptors
        {{ extract_descriptor('v:programEvaluationReference:programEvaluationPeriodDescriptor::string') }} as program_evaluation_period_descriptor,
        {{ extract_descriptor('v:programEvaluationReference:programEvaluationTypeDescriptor::string') }}   as program_evaluation_type_descriptor,
        {{ extract_descriptor('v:programEvaluationReference:programTypeDescriptor::string') }}             as program_type_descriptor,
        {{ extract_descriptor('v:summaryEvaluationRatingLevelDescriptor::string') }}                       as summary_evaluation_rating_level,

        -- references
        v:educationOrganizationReference as education_organization_reference,
        v:programEvaluationReference     as program_evaluation_reference,
        v:staffEvaluatorStaffReference   as staff_evaluator_staff_reference,
        v:studentReference               as student_reference,

        -- lists
        v:externalEvaluators          as v_external_evaluators,
        v:studentEvaluationElements   as v_student_evaluation_elements,
        v:studentEvaluationObjectives as v_student_evaluation_objectives,

        -- ed-fi extensions
        v:_ext as v_ext

    from student_program_evaluations
)

select * from renamed