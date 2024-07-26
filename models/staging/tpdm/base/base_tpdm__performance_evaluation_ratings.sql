with performance_evaluation_ratings as (
    {{ source_edfi3('performance_evaluation_ratings') }}
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

        v:id::string                                                        as record_guid,
        v:performanceEvaluationReference:educationOrganizationId::int       as ed_org_id,
        v:performanceEvaluationReference:performanceEvaluationTitle::string as performance_evaluation_title,
        v:performanceEvaluationReference:schoolYear::string                 as school_year,
        v:personReference:person_id::string                                 as person_id,
        v:actualTime::time                                                  as actual_time,
        v:scheduleDate::date                                                as schedule_date,
        v:actualDate::date                                                  as actual_date,
        v:comments::string                                                  as comments,
        v:actualDuration::string                                            as actual_duration,
        v:announced::boolean                                                as is_announced,
        -- descriptors
        {{ extract_descriptor('v:performanceEvaluationRatingLevelDescriptor::string') }} as performance_evaluation_rating_level,
        {{ extract_descriptor('v:coteachingStyleObservedDescriptor::string') }}          as coteaching_style_observed,
        -- unflattened lists
        v:reviewers as reviewers,
        v:results   as results,
        -- references
        v:personReference                as person_reference,
        v:performanceEvaluationReference as performance_evaluation_reference
    from performance_evaluation_ratings
)
select * from renamed
