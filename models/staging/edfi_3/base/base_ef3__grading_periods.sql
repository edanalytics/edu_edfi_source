with grading_periods as (
    {{ source_edfi3('grading_periods') }}
),
renamed as (
    select
        tenant_code,
        api_year,
        pull_timestamp,
        file_row_number,
        filename,
        is_deleted,
        v:id::string as record_guid,
        {{ extract_descriptor('v:gradingPeriodDescriptor::string') }} as grading_period,
        v:periodSequence::int                     as period_sequence,
        v:schoolReference:schoolId::int           as school_id,
        v:schoolYearTypeReference:schoolYear::int as school_year,
        v:beginDate::date                         as begin_date,
        v:endDate::date                           as end_date,
        v:totalInstructionalDays::float           as total_instructional_days,
        -- references
        v:schoolReference as school_reference,

        -- edfi extensions
        v:_ext as v_ext
    from grading_periods
)
select * from renamed