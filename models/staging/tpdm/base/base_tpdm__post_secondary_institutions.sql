with post_secondary_institutions as (
    {{ source_edfi3('post_secondary_institutions') }}
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

        v:id::string                      as record_guid,
        v:postSecondaryInstitutionId::int as post_secondary_institution_id,
        -- descriptors
        {{ extract_descriptor('v:postSecondaryInstitutionLevelDescriptor::string')}} as post_secondary_institution_level,
        {{ extract_descriptor('v:administrativeFundingControlDescriptor::string')}}  as administrative_funding_control,
        {{ extract_descriptor('v:federalLocaleCodeDescriptor::string')}}             as federal_locale_code,
        -- unflattened lists
        v:mediumOfInstructions as v_medium_of_instructions
    from student_academic_records
)

select * from renamed
