with seas as (
    {{ source_edfi3('state_education_agencies') }}
),
renamed as (
    select
        tenant_code,
        api_year,
        pull_timestamp,
        file_row_number,
        filename,
        is_deleted,
        -- fields
        v:id::string                     as record_guid,
        v:stateEducationAgencyId::int    as sea_id,
        v:nameOfInstitution::string      as sea_name,
        v:shortNameOfInstitution::string as sea_short_name,
        v:webSite::string                as website,
        -- descriptors
        {{ extract_descriptor('v:operationalStatusDescriptor::string') }} as operational_status,
        -- lists
        v:accountabilities       as v_accountabilities
        v:addresses              as v_addresses,
        v:categories             as v_categories,
        v:federalFunds           as v_federal_funds,
        v:identificationCodes    as v_identification_codes,
        v:indicators             as v_indicators,
        v:institutionTelephones  as v_institution_telephones,
        v:internationalAddresses as v_international_addresses,

        -- edfi extensions
        v:_ext as v_ext
    from seas
) 
select * from renamed