with educator_prep_programs as (
    {{ source_edfi3('evaluation_element_rating') }}
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

        v:areaOfRefinement::string    as area_of_refinement,
        v:areaOfReinforcement::string as area_of_reinforcement,
        v:comments::string            as comments,
        v:feedback::string            as feedback,
        -- descriptors
        {{ extract_descriptor('v:evaluationElementRatingLevelDescriptor::string') }} as evaluation_element_rating_level,
        -- unflattened lists
        v:elementRatingResults  as element_rating_results,
        -- references
        v:evaluationElementReference         as evaluation_element_reference,
        v:evaluationObjectiveRatingReference as evaluation_objective_rating_reference
    from educator_prep_programs
)
select * from renamed
