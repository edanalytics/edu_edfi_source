with certification_exams as (
    {{ source_edfi3('certification_exams') }}
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

        v:id::string                          as record_guid,
        v:certificationExamIdentifier::string as certification_exam_id,
        v:namespace::string                   as namespace,
        v:certificationExamTitle::string      as certification_exam_title,
        v:effectiveDate::date                 as effective_date,
        v:endDate::date                       as end_date,
        -- descriptors
        {{ extract_descriptor('v:certificationExamTypeDescriptor::string') }} as certification_exam_type
    from certification_exams
)
select * from renamed
