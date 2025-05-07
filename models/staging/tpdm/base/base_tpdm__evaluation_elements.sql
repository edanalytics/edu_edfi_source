
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

        v:id::string as record_guid,
        -- identity components
        v:evaluationElementTitle::string                                  as evaluation_element_title,
        v:evaluationObjectiveReference:educationOrganizationId::int       as ed_org_id,
        v:evaluationObjectiveReference:evaluationObjectiveTitle::string   as evaluation_objective_title,
        v:evaluationObjectiveReference:evaluationTitle::string            as evaluation_title,
        v:evaluationObjectiveReference:performanceEvaluationTitle::string as performance_evaluation_title,
        v:evaluationObjectiveReference:schoolYear::int                    as school_year,
        {{ extract_descriptor('v:evaluationObjectiveReference:evaluationPeriodDescriptor::string') }}          as evaluation_period,
        {{ extract_descriptor('v:evaluationObjectiveReference:performanceEvaluationTypeDescriptor::string') }} as performance_evaluation_type,
        {{ extract_descriptor('v:evaluationObjectiveReference:termDescriptor::string') }}                      as academic_term,
        -- non-identity components
        {{ extract_descriptor('v:evaluationTypeDescriptor::string') }} as evaluation_type,
        v:minRating::float as min_rating,
        v:maxRating::float as max_rating,
        v:sortOrder::int   as sort_order,
        -- unflattened lists
        v:ratingLevels as v_rating_levels,
        -- references
        v:evaluationObjectiveReference as evaluation_objective_reference,
        -- edfi extensions
        v:_ext as v_ext
    from evaluation_elements
)
select * from renamed
