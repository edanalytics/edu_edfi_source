with certification_exam_results as (
    {{ source_edfi3('certification_exam_results') }}
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

        v:id::string                                                     as record_guid,
        v:certificationExamReference:certificationExamIdentifier::string as certification_exam_id,
        v:personReference:personId::string                               as person_id,
        v:certificationExamDate::date                                    as certification_exam_date,
        v:attemptNumber::int                                             as attempt_number,
        v:certificationExamPassIndicator::boolean                        as certification_exam_pass_indicator,
        -- descriptors
        {{ extract_descriptor('v:personReference:sourceSystemDescriptor::string') }} as person_source_system_descriptor,
        -- references
        v:certificationExamReference as certification_exam_reference,
        v:personReference            as person_reference
    from certification_exam_results
)
select * from renamed
