with contacts as (
    {{ source_edfi3('contacts') }}
),
renamed as (
    select
        tenant_code,
        api_year,
        pull_timestamp,
        file_row_number,
        last_modified_timestamp,
        filename,
        is_deleted,

        v:id::string                                                                     as record_guid,
        ods_version,
        data_model_version,
        v:contactUniqueId::string                                                        as contact_unique_id,
        v:personReference:personId::string                                               as person_id,
        v:firstName::string                                                              as first_name,
        v:middleName::string                                                             as middle_name,
        v:lastSurname::string                                                            as last_name,
        v:maidenName::string                                                             as maiden_name,
        v:generationCodeSuffix::string                                                   as generation_code_suffix,
        v:personalTitlePrefix::string                                                    as personal_title_prefix,
        v:genderIdentity::string                                                         as gender_identity,
        v:preferredFirstName::string                                                     as preferred_first_name,
        v:preferredLastSurname::string                                                   as preferred_last_name,
        v:loginId::string                                                                as login_id,
        {{ extract_descriptor('v:sexDescriptor::string') }}                              as sex,
        {{ extract_descriptor('v:highestCompletedLevelOfEducationDescriptor::string') }} as highest_completed_level_of_education,
        {{ extract_descriptor('v:personReference:sourceSystemDescriptor::string') }}     as person_source_system,
        -- references
        v:personReference                 as person_reference,
        -- unflattened lists
        v:addresses                       as v_addresses,
        v:internationalAddresses          as v_international_addresses,
        v:electronicMails                 as v_electronic_mails,
        v:telephones                      as v_telephones,
        v:languages                       as v_languages,
        v:otherNames                      as v_other_names,
        v:personalIdentificationDocuments as v_personal_identification_documents,

        -- edfi extensions
        v:_ext as v_ext
    from contacts
)
select * from renamed