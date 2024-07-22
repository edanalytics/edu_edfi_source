with fieldwork_experiences as (
    {{ source_edfi3('fieldwork_experiences') }}
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

        v:id::string                                                       as record_guid,
        v:fieldworkIdentifier::string                                      as fieldwork_id,
        v:educatorPreparationProgramReference:educationOrganizationId::int as ed_org_id,
        v:educatorPreparationProgramReference:link:rel::string             as ed_org_type,
        v:studentReference:studentUniqueId::string                         as student_id,
        v:beginDate::date                                                  as begin_date,
        v:endDate::date                                                    as end_date,
        -- descriptors
        {{ extract_descriptor('v:fieldworkTypeDescriptor::string') }} as fieldwork_type,
        -- references
        v:educatorPreparationProgramReference as educator_preparation_program_reference,
        v:studentReference                    as student_reference
    from fieldwork_experiences
)
select * from renamed
