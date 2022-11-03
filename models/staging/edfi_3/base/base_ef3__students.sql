with students as (
    {{ source_edfi3('students') }}
),
renamed as (
    select
        tenant_code,
        api_year,
        pull_timestamp,
        file_row_number,
        filename,
        is_deleted,
        v:id::string                         as record_guid,
        v:studentUniqueId::string            as student_unique_id,
        v:firstName::string                  as first_name,
        v:middleName::string                 as middle_name,
        v:lastSurname::string                as last_name,
        v:generationCodeSuffix::string       as generation_code_suffix,
        v:maidenName::string                 as maiden_name,
        v:personalTitlePrefix::string        as personal_title_prefix,
        v:personReference:personId::string   as person_id,
        v:birthDate::date                    as birth_date,
        v:birthCity::string                  as birth_city,
        v:birthInternationalProvince::string as birth_international_province,
        v:dateEnteredUS::date                as date_entered_us,
        v:multipleBirthStatus::boolean       as is_multiple_birth,
        -- descriptors
        {{ extract_descriptor('v:personReference:sourceSystemDescriptor::string') }} as person_source_system,
        {{ extract_descriptor('v:birthSexDescriptor::string') }}                     as birth_sex,
        {{ extract_descriptor('v:citizenshipStatusDescriptor::string') }}            as citizenship_status,
        {{ extract_descriptor('v:birthStateAbbreviationDescriptor::string') }}       as birth_state,
        {{ extract_descriptor('v:birthCountryDescriptor::string') }}                 as birth_country,
        -- nested lists
        v:identificationDocuments         as v_identification_documents,
        v:otherNames                      as v_other_names,
        v:personalIdentificationDocuments as v_personal_identification_documents,
        v:visas                           as v_visas,

        -- edfi extensions
        v:_ext as v_ext
    from students
)
select * from renamed