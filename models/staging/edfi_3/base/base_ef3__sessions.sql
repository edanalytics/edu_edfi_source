with sessions as (
    {{ source_edfi3('sessions') }}
),
renamed as (
    select
        tenant_code,
        api_year,
        pull_timestamp,
        file_row_number,
        filename,
        is_deleted,
        v:id::string                                  as record_guid,
        v:schoolReference:schoolId::integer           as school_id,
        v:schoolYearTypeReference:schoolYear::integer as school_year,
        v:beginDate::date                             as session_begin_date,
        v:endDate::date                               as session_end_date,
        v:sessionName::string                         as session_name,
        v:totalInstructionalDays::float               as total_instructional_days,
        {{ extract_descriptor('v:termDescriptor::string') }} as academic_term,
        -- references
        v:schoolReference as school_reference,
        -- lists
        v:academicWeeks as v_academic_weeks,
        v:gradingPeriods as v_grading_periods,

        -- edfi extensions
        v:_ext as v_ext
    from sessions
)
select * from renamed