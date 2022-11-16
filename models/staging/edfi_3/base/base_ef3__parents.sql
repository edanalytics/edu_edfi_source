with parents as (
    {{ source_edfi3('parents') }}
),
renamed as (
    select
        tenant_code,
        api_year,
        pull_timestamp,
        file_row_number,
        filename,
        is_deleted,

        v:id::string                                        as record_guid,
        v:parentUniqueId::string                            as parent_unique_id,
        v:firstName::string                                 as first_name,
        v:middleName::string                                as middle_name,
        v:lastSurname::string                               as last_name,
        v:maidenName::string                                as maiden_name,
        v:generationCodeSuffix::string                      as generation_code_suffix,
        v:personalTitlePrefix::string                       as personal_title_prefix,
        v:loginId::string                                   as login_id,
        {{ extract_descriptor('v:sexDescriptor::string') }} as sex,
        -- unflattened lists
        v:addresses                       as v_addresses,
        v:internationalAddresses          as v_international_addresses,
        v:electronicMails                 as v_emails,
        v:telephones                      as v_telephones,
        v:languages                       as v_languages,
        v:otherNames                      as v_other_names,
        v:personalIdentificationDocuments as v_personal_identification_documents,

        -- edfi extensions
        v:_ext as v_ext
    from parents
)
select * from renamed