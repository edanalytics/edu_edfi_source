with educator_prep_programs as (
    {{ source_edfi3('evaluation_element') }}
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

        v:evaluationElementTitle::string as evaluation_element_title,
        v:sortOrder::int                 as sort_order,
        v:minRating::decimal(6,3)        as min_rating,
        v:maxRating::decimal(6,3)        as max_rating,
        -- descriptors
        {{ extract_descriptor('v:evaluationTypeDescriptor::string') }} as evaluation_type,
        -- unflattened lists
        v:ratingLevels as rating_levels,
        -- references
        v:evaluationObjectiveReference as evaluation_objective_reference
    from educator_prep_programs
)
select * from renamed
