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

        v:id::string                                                 as record_guid,
        v:evaluationElementReference:educationOrganizationId::int    as ed_org_id,
        v:evaluationElementReference:evaluationElementTitle::int     as evaluation_element_title,
        v:evaluationElementReference:evaluationObjectiveTitle::int   as evaluation_objective_title,
        v:evaluationElementReference:evaluationTitle::int            as evaluation_title,
        v:evaluationElementReference:performanceEvaluationTitle::int as performance_evaluation_title,
        v:evaluationElementReference:schoolYear::int                 as schoolYear,
        v:rubricRating::int                                          as rubric_rating,
        v:criterionDescription::string                               as criterion_description,
        v:dimensionOrder::int                                        as dimension_order,
        -- descriptors
        {{ extract_descriptor('v:rubricRatingLevelDescriptor::string') }} as rubric_rating_level,
        -- references
        v:evaluationElementReference as evaluation_element_reference
    from rubric_dimensions
)
select * from renamed
