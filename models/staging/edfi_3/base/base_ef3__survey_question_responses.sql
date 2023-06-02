with survey_question_responses as (
    {{ source_edfi3('survey_question_responses') }}
),
renamed as (
    select 
        tenant_code,
        api_year,
        pull_timestamp,
        file_row_number,
        filename,
        is_deleted,

        v:id::string                                                  as record_guid,
        v:surveyQuestionReference:namespace::string                   as namespace,
        v:surveyQuestionReference:surveyIdentifier::string            as survey_id,
        v:surveyQuestionReference:questionCode::string                as question_code,
        v:surveyResponseReference:surveyResponseIdentifier::string    as survey_response_id,
        v:comment::string                                             as comment,
        v:noResponse::boolean                                         as no_response,
        --references
        v:surveyQuestionReference   as survey_question_reference,
        v:surveyResponseReference   as survey_response_reference,
        -- lists
        v:surveyQuestionMatrixElementResponses  as v_survey_question_matrix_element_responses,
        v:values                                as v_values,
        -- edfi extensions
        v:_ext as v_ext
    from survey_question_responses
)
select * from renamed