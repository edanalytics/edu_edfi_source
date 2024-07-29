with evaluation_objective_ratings as (
    {{ source_edfi3('evaluation_objective_ratings') }}
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
        v:evaluationObjectiveReference:educationOrganizationId::int                                            as ed_org_id,
        v:evaluationRatingReference:evaluationDate::timestamp                                                  as evaluation_date,
        v:evaluationObjectiveReference:evaluationObjectiveTitle                                                as evaluation_objective_title,
        {{ extract_descriptor('v:evaluationObjectiveReference:evaluationPeriodDescriptor::string') }}          as evaluation_period,
        v:evaluationObjectiveReference:evaluationTitle::string                                                 as evaluation_title,
        v:evaluationObjectiveReference:performanceEvaluationTitle                                              as perfomance_evaluation_title,
        {{ extract_descriptor('v:evaluationObjectiveReference:performanceEvaluationTypeDescriptor::string') }} as performance_evaluation_type,
        v:evaluationRatingReference:personId::string                                                           as person_id,
        v:evaluationObjectiveReference:schoolYear::int                                                         as school_year,
        {{ extract_descriptor('v:evaluationRatingReference:sourceSystemDescriptor::string')}}                  as source_system,
        {{ extract_descriptor('v:evaluationRatingReference:termDescriptor::string')}}                          as academic_term,
        -- non-identity components
        v:comments::string as comments,
        -- descriptors
        {{ extract_descriptor('v:objectiveRatingLevelDescriptor::string') }} as objective_rating_level,
        -- unflattened lists
        v:objectiveRatingResults as v_objective_rating_results,
        v:results                as v_results,
        -- references
        v:evaluationRatingReference    as evaluation_rating_reference,
        v:evaluationObjectiveReference as evaluation_objective_reference,
        -- edfi extensions
        v:_ext as v_ext
    from evaluation_objective_ratings
)
select * from renamed
