with evaluation_objectives as (
    {{ source_edfi3('evaluation_objectives') }}
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
        v:minRating::float                                 as min_rating,
        v:maxRating::float                                 as max_rating,
        -- descriptors
        {{ extract_descriptor('v:evaluationTypeDescriptor::string') }} as evaluation_type,
        -- unflattened lists
        v:ratingLevels as v_rating_levels,
        -- references
        v:evaluationReference as evaluation_reference
    from evaluation_objectives
)
select * from renamed
