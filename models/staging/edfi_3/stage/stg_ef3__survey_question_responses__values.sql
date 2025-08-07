with stg_survey_question_responses as (
    select * from {{ ref('stg_ef3__survey_question_responses') }}
),
flattened as (
    select
        tenant_code,
        api_year,
        k_survey,
        k_survey_question,
        k_survey_response,
        value:surveyQuestionResponseValueIdentifier::float as question_response_value_id,
        value:numericResponse::float  as numeric_response,
        value:textResponse::string    as text_response
    from stg_survey_question_responses
        {{ json_flatten('v_values') }}
)
select * from flattened
