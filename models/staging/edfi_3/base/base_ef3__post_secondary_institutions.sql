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
        v:nameOfInstitution::string       as name_of_institution,
        v:shortNameOfInstitution::string  as short_name_of_institution,
        v:webSite::string                 as web_site,
        -- descriptors
        {{ extract_descriptor('v:postSecondaryInstitutionLevelDescriptor::string')}} as post_secondary_institution_level,
        {{ extract_descriptor('v:administrativeFundingControlDescriptor::string')}}  as administrative_funding_control,
        {{ extract_descriptor('v:federalLocaleCodeDescriptor::string')}}             as federal_locale_code,
        {{ extract_descriptor('v:operationalStatusDescriptor::string')}}             as operational_status_descriptor,
        -- unflattened lists
        v:categories             as v_categories,
        v:addresses              as v_addresses,
        v:identificationCodes    as v_identification_codes,
        v:indicators             as v_indicators,
        v:institutionTelephones  as v_institution_telephones,
        v:internationalAddresses as v_international_addresses,
        v:mediumOfInstructions   as v_medium_of_instructions
    from post_secondary_institutions
)
select * from renamed
