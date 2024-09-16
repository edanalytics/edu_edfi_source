with src_staffs as (
    {{ source_edfi3('staffs') }}
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
        -- fields
        v:id::string                                 as record_guid,
        v:staffUniqueId::string                      as staff_unique_id,
        v:loginId::string                            as login_id,
        v:firstName::string                          as first_name,
        v:lastSurname::string                        as last_name,
        v:maidenName::string                         as maiden_name,
        v:middleName::string                         as middle_name,
        v:generationCodeSuffix::string               as generation_code_suffix,
        v:personalTitlePrefix::string                as personal_title_prefix,
        v:genderIdentity::string                     as gender_identity,
        v:preferredFirstName::string                 as preferred_first_name,
        v:preferredLastSurname::string               as preferred_last_name,
        v:personReference:personId::string           as person_id,
        v:birthDate::date                            as birth_date,
        v:hispanicLatinoEthnicity::boolean           as has_hispanic_latino_ethnicity,
        v:highlyQualifiedTeacher::boolean            as is_highly_qualified_teacher,
        v:yearsOfPriorProfessionalExperience::float  as years_of_prior_professional_experience,
        v:yearsOfPriorTeachingExperience::float      as years_of_prior_teaching_experience,
        -- descriptors
        {{ extract_descriptor('v:sexDescriptor::string') }}                                 as gender,
        {{ extract_descriptor('v:highestCompletedLevelOfEducationDescriptor::string') }}    as highest_completed_level_of_education,
        {{ extract_descriptor('v:citizenshipStatusDescriptor::string') }}                   as citizenship_status,
        {{ extract_descriptor('v:personReference:sourceSystemDescriptor::string') }}        as person_source_system,
        {{ extract_descriptor('v:oldEthnicityDescriptor:sourceSystemDescriptor::string') }} as old_ethnicity,
        -- references
        v:personReference                 as person_reference,
        -- lists
        v:addresses                       as v_addresses,
        v:ancestryEthnicOrigins           as v_ancestry_ethnic_origins,
        v:credentials                     as v_credentials,
        v:electronicMails                 as v_electronic_mails,
        v:identificationCodes             as v_identification_codes,
        v:identificationDocuments         as v_identification_documents,
        v:personalIdentificationDocuments as v_personal_identification_documents,
        v:internationalAddresses          as v_international_addresses,
        v:languages                       as v_languages,
        v:otherNames                      as v_other_names,
        v:races                           as v_races,
        v:recognitions                    as v_recognitions,
        v:telephones                      as v_telephones,
        v:tribalAffiliations              as v_tribal_affiliations,
        v:visas                           as v_visas,

        -- edfi extensions
        v:_ext as v_ext
    from src_staffs
)
select * from renamed