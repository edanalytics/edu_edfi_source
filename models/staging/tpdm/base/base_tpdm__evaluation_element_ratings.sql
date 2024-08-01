with evaluation_element_ratings as (
    {{ source_edfi3('evaluation_element_ratings') }}
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
        v:evaluationElementReference:educationOrganizationId::int                                            as ed_org_id,
        v:evaluationObjectiveRatingReference:evaluationDate::timestamp                                       as evaluation_date,
        v:evaluationElementReference:evaluationElementTitle::string                                          as evaluation_element_title,
        v:evaluationElementReference:evaluationObjectiveTitle::string                                        as evaluation_objective_title,
        {{ extract_descriptor('v:evaluationElementReference:evaluationPeriodDescriptor::string') }}          as evaluation_period,
        v:evaluationElementReference:evaluationTitle::string                                                 as evaluation_title,
        v:evaluationElementReference:performanceEvaluationTitle::string                                      as perfomance_evaluation_title,
        {{ extract_descriptor('v:evaluationElementReference:performanceEvaluationTypeDescriptor::string') }} as performance_evaluation_type,
        v:evaluationObjectiveRatingReference:personId::string                                                as person_id,
        v:evaluationElementReference:schoolYear::int                                                         as school_year,
        {{ extract_descriptor('v:evaluationObjectiveRatingReference:sourceSystemDescriptor::string') }}      as source_system,
        {{ extract_descriptor('v:evaluationObjectiveRatingReference:termDescriptor::string') }}              as academic_term,
        -- non-identity components
        v:areaOfRefinement::string    as area_of_refinement,
        v:areaOfReinforcement::string as area_of_reinforcement,
        v:comments::string            as comments,
        v:feedback::string            as feedback,
        -- descriptors
        {{ extract_descriptor('v:evaluationElementRatingLevelDescriptor::string') }} as evaluation_element_rating_level,
        -- unflattened lists
        v:elementRatingResults  as v_element_rating_results,
        -- references
        v:evaluationElementReference         as evaluation_element_reference,
        v:evaluationObjectiveRatingReference as evaluation_objective_rating_reference,
        -- edfi extensions
        v:_ext as v_ext
    from evaluation_element_ratings
)
select * from renamed
