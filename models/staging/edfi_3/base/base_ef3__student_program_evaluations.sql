
with base_ef3__student_program_evaluations as (
    select * from {{ source('student_program_evaluations') }}
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

        -- generic
        v:id::string                                                                 as record_guid,
        v:evaluationDate::date                                                       as evaluation_date,
        v:evaluationDuration::int                                                    as evaluation_duration,
        v:summaryEvaluationNumericRating::float                                      as summary_evaluation_numeric_rating,
        v:summaryEvaluationComment::string                                           as summary_evaluation_comment,

        -- references
        v:programEvaluationReference                                                 as program_evaluation_reference,
        v:studentReference                                                           as student_reference,
        v:educationOrganizationReference                                             as education_organization_reference,
        v:staffEvaluatorStaffReference                                               as staff_evaluator_staff_reference,

        -- descriptors
        {{ extract_descriptor('v:summaryEvaluationRatingLevelDescriptor::string') }} as summary_evaluation_rating_level,

        -- lists
        v:externalEvaluators                                                         as v_external_evaluators,
        v:studentEvaluationObjectives                                                as v_student_evaluation_objectives,
        v:studentEvaluationElements                                                  as v_student_evaluation_elements,

        -- ed-fi extensions
        v:_ext                                                                       as v_ext,

    from base_ef3__student_program_evaluations
)

select * from renamed
