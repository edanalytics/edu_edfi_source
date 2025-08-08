with survey_section_aggregate_responses as (
    {{ source_edfi3('survey_section_aggregate_responses') }}
),
renamed as (
    select
        tenant_code,
        api_year,
        pull_timestamp,
        last_modified_timestamp,
        file_row_number,
        filename,
        is_deleted,

        v:id::string as record_guid,
        -- identity components
        v:evaluationElementRatingReference:educationOrganizationId::string                                         as ed_org_id,
        v:evaluationElementRatingReference:evaluationDate::string                                                  as evaluation_date,
        v:evaluationElementRatingReference:evaluationElementTitle::string                                          as evaluation_element_title,
        {{ extract_descriptor('v:evaluationElementRatingReference:sourceSystemDescriptor::string') }}              as evaluation_element_rating_source_system,
        v:evaluationElementRatingReference:evaluationObjectiveTitle::string                                        as evaluation_objective_title,
        {{ extract_descriptor('v:evaluationElementRatingReference:evaluationPeriodDescriptor::string') }}          as evaluation_period,
        v:evaluationElementRatingReference:evaluationTitle::string                                                 as evaluation_title,
        v:evaluationElementRatingReference:performanceEvaluationTitle::string                                      as performance_evaluation_title,
        {{ extract_descriptor('v:evaluationElementRatingReference:performanceEvaluationTypeDescriptor::string') }} as performance_evaluation_type,
        v:evaluationElementRatingReference:personId::string                                                        as person_id,
        v:evaluationElementRatingReference:schoolYear::string                                                      as school_year,
        {{ extract_descriptor('v:evaluationElementRatingReference:termDescriptor::string') }}                      as academic_term,
        v:surveySectionReference:surveyIdentifier::string                                                          as survey_identifier,
        v:surveySectionReference:namespace::string                                                                 as survey_section_namespace,
        v:surveySectionReference:surveySectionTitle::string                                                        as survey_section_title,
        -- non-identity components
        v:scoreValue::float as score_value,
        -- references
        v:evaluationElementRatingReference as evaluation_element_rating_reference,
        v:surveySectionReference           as survey_section_reference,
        -- edfi extensions
        v:_ext as v_ext 
    from survey_section_aggregate_responses
)
select * from renamed
