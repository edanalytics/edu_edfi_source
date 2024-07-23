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

        v:id::string       as record_guid,
        v:comments::string as comments,
        -- descriptors
        {{ extract_descriptor('v:objectiveRatingLevelDescriptor::string') }} as objective_rating_level,
        -- unflattened lists
        v:objectiveRatingResults  as v_objective_rating_results,
        v:results                 as v_results,
        -- references
        v:evaluationRatingReference    as evaluation_rating_reference,
        v:evaluationObjectiveReference as evaluation_objective_reference
    from evaluation_objective_ratings
)
select * from renamed
