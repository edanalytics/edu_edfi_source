with evaluation_ratings as (
    {{ source_edfi3('evaluation_ratings') }}
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

        v:id::string                                  as record_guid,
        v:sectionReference:sectionIdentifier::string  as section_id,
        v:evaluationDate::date                        as evaluation_date,
        v:comments::string                            as comments,
        v:actualDuration::int                         as actual_duration,
        -- descriptors
        {{ extract_descriptor('v:evaluationRatingStatusDescriptor::string') }} as evaluation_rating_status,
        {{ extract_descriptor('v:evaluationRatingLevelDescriptor::string') }}  as evaluation_rating_level,
        -- unflattened lists
        v:reviewers as reviewers,
        v:results   as results,
        -- references
        v:evaluationReference                  as evaluation_reference,
        v:performanceEvaluationRatingReference as performance_evaluation_rating_reference,
        v:sectionReference                     as section_reference
    from evaluation_ratings
)
select * from renamed
