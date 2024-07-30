with rubric_dimensions as (
    {{ source_edfi3('rubric_dimensions') }}
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
        v:evaluationElementReference:evaluationElementTitle::string                                          as evaluation_element_title,
        v:evaluationElementReference:evaluationObjectiveTitle::string                                        as evaluation_objective_title,
        {{ extract_descriptor('v:evaluationElementReference:evaluationPeriodDescriptor::string') }}          as evaluation_period,
        v:evaluationElementReference:evaluationTitle::string                                                 as evaluation_title,
        v:evaluationElementReference:performanceEvaluationTitle::string                                      as performance_evaluation_title,
        {{ extract_descriptor('v:evaluationElementReference:performanceEvaluationTypeDescriptor::string') }} as performance_evaluation_type,
        v:rubricRating::int                                                                                  as rubric_rating,
        v:evaluationElementReference:schoolYear::int                                                         as school_year,
        {{ extract_descriptor('v:evaluationElementReference:termDescriptor::string') }}                      as academic_term,
        -- non-identity components
        v:criterionDescription::string as criterion_description,
        v:dimensionOrder::int          as dimension_order,
        -- descriptors
        {{ extract_descriptor('v:rubricRatingLevelDescriptor::string') }} as rubric_rating_level,
        -- references
        v:evaluationElementReference as evaluation_element_reference
    from rubric_dimensions
)
select * from renamed
