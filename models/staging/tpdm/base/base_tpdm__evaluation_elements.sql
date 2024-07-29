with evaluation_elements as (
    {{ source_edfi3('evaluation_elements') }}
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

        v:id::string                                                                                           as record_guid,
        -- identity components
        v:evaluationObjectiveReference:educationOrganizationId::int                                            as ed_org_id,
        v:evaluationElementTitle::string                                                                       as evaluation_element_title,
        v:evaluationObjectiveReference:evaluationObjectiveTitle::string                                        as evaluation_objective_title,
        {{ extract_descriptor('v:evaluationObjectiveReference:evaluationPeriodDescriptor::string') }}          as evaluation_period,
        v:evaluationObjectiveReference:evaluationTitle::string                                                 as evaluation_title,
        v:evaluationObjectiveReference:performanceEvaluationTitle::string                                      as performance_evaluation_title,
        {{ extract_descriptor('v:evaluationObjectiveReference:performanceEvaluationTypeDescriptor::string') }} as performance_evaluation_type,
        v:evaluationObjectiveReference:schoolYear::int                                                         as school_year,
        {{ extract_descriptor('v:evaluationObjectiveReference:termDescriptor::string') }}                      as academic_term,
        -- non-identity components
        v:sortOrder::int   as sort_order,
        v:minRating::float as min_rating,
        v:maxRating::float as max_rating,
        -- descriptors
        {{ extract_descriptor('v:evaluationTypeDescriptor::string') }} as evaluation_type,
        -- unflattened lists
        v:ratingLevels as v_rating_levels,
        -- references
        v:evaluationObjectiveReference as evaluation_objective_reference
        -- edfi extensions
        v:_ext as v_ext
    from evaluation_elements
)
select * from renamed
