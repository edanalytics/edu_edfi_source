with networks as (
    {{ source_edfi3('education_organization_networks') }}
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
        v:id::string                              as record_guid,
        ods_version,
        data_model_version,
        v:educationOrganizationNetworkId::int     as network_id,
        v:nameOfInstitution::string               as network_name,
        v:shortNameOfInstitution::string          as network_short_name,
        v:webSite::string                         as website,
        -- descriptors
        {{ extract_descriptor('v:networkPurposeDescriptor::string') }}    as network_purpose,
        {{ extract_descriptor('v:operationalStatusDescriptor::string') }} as operational_status,
        -- unflattened lists
        v:categories                              as v_categories,
        v:addresses                               as v_addresses,
        v:identificationCodes                     as v_identification_codes,
        v:indicators                              as v_indicators,
        v:institutionTelephones                   as v_institution_telephones,
        v:internationalAddresses                  as v_international_addresses,
        -- edfi extensions
        v:_ext as v_ext
    from networks
)
select * from renamed
