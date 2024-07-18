with evaluation_objective as (
    {{ source_edfi3('evaluation_objective') }}
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

        v:id::string                                       as record_guid,
        v:evaluationReference:educationOrganizationId::int as ed_org_id,
        v:evaluationObjectiveTitle::string                 as evaluation_objective_title,
        v:evaluationObjectiveDescription::string           as evaluation_objective_description,
        v:sortOrder::int                                   as sort_order,
        v:minRating::decimal(6,3)                          as min_rating,
        v:maxRating::decimal(6,3)                          as max_rating,
        -- descriptors
        {{ extract_descriptor('v:evaluationTypeDescriptor::string') }} as evaluation_type,
        -- unflattened lists
        v:ratingLevels as rating_levels,
        -- references
        v:evaluationReference as evaluation_reference
    from evaluation_objective
)
select * from renamed
