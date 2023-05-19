with stg_survey_questions as (
    select * from {{ ref('stg_ef3__survey_questions') }}
),
flattened as (
    select 
        tenant_code,
        api_year,
        k_survey,
        k_survey_question,
        value:numericValue::float as numeric_value,
        value:sortOrder::int      as sort_order,
        value:textValue::string   as text_value
    from stg_survey_questions
        , lateral flatten(input=>v_response_choices)
)
select * from flattened
