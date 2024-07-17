with rubric_ratings as (
    {{ source_edfi3('rubric_ratings') }}
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

        v:id::string                                              as record_guid,
        v:evaluationElementReference:educationOrganizationId::int as ed_org_id,
        v:rubricRating::int                                       as rubric_rating,
        v:criterionDescription::string                            as criterion_description,
        v:dimensionOrder::int                                     as dimension_order,
        -- descriptors
        {{ extract_descriptor('v:rubricRatingLevelDescriptor::string') }} as rubric_rating_level,
        -- references
        v:evaluationElementReference as evaluation_element_reference
    from rubric_ratings
)
select * from renamed
