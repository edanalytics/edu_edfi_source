with candidates as (
    {{ source_edfi3('candidates') }}
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

        v:id::string                         as record_guid,
        v:candidateIdentifier::string        as candidate_id,
        v:personReference:personId::string  as person_id,
        v:firstName::string                  as first_name,
        v:lastSurname::string                as last_name,
        v:middleName::string                 as middle_name,
        v:maidenName::string                 as maiden_name,
        v:generationCodeSuffix::string       as generation_code_suffix,
        v:personalTitlePrefix::string        as personal_title_prefix,
        v:preferredFirstName::string         as preferred_first_name,
        v:preferredLastSurname::string       as preferred_last_name,
        v:birthCity::string                  as birth_city,
        v:birthDate::date                    as birth_date,
        v:birthInternationalProvince::string as birth_international_province,
        v:dateEnteredUS::date                as date_entered_us,
        v:displacementStatus::string         as displacement_status,
        v:economicDisadvantaged::boolean     as is_economic_disadvantaged,
        v:firstGenerationStudent::boolean    as is_first_generation_student,
        v:hispanicLatinoEthnicity::boolean   as has_hispanic_latino_ethnicity,
        v:multipleBirthStatus::boolean       as is_multiple_birth,
        -- descriptors
        {{ extract_descriptor('v:genderDescriptor::string') }}                    as gender,
        {{ extract_descriptor('v:sexDescriptor::string') }}                       as sex,
        {{ extract_descriptor('v:birthSexDescriptor::string', 'sexDescriptor') }} as birth_sex,
        {{ extract_descriptor('v:birthStateAbbreviationDescriptor::string', 'stateAbbreviationDescriptor') }} as birth_state,
        {{ extract_descriptor('v:birthCountryDescriptor::string', 'countryDescriptor') }} as birth_country,
        {{ extract_descriptor('v:englishLanguageExamDescriptor::string' )}}       as english_language_exam,
        {{ extract_descriptor('v:limitedEnglishProficiencyDescriptor::string' )}} as lep_code,
        -- unflattened lists
        v:addresses                       as v_addresses,
        v:disabilities                    as v_disabilities,
        v:electronicMails                 as v_emails,
        v:languages                       as v_languages,
        v:otherNames                      as v_other_names,
        v:personalIdentificationDocuments as v_personal_identification_documents,
        v:races                           as v_races,
        v:telephones                      as v_telephones,
        -- references
        v:personReference as person_reference,
        -- edfi extensions
        v:_ext as v_ext
    from candidates
)
select * from renamed
