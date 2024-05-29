with service_centers as (
    {{ source_edfi3('education_service_centers') }}
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
        v:id::string                                            as record_guid,
        v:educationServiceCenterId::string                      as esc_id,
        v:nameOfInstitution::string                             as esc_name,
        v:shortNameOfInstitution::string                        as esc_short_name,
        v:stateEducationAgencyReference:stateEducationAgencyId  as sea_id,
        v:webSite::string                                       as website,
        -- descriptors
        {{ extract_descriptor('v:operationalStatusDescriptor::string') }} as operational_status,
        -- unflattened lists
        v:addresses                         as v_addresses,
        v:categories                        as v_categories,
        v:identificationCodes               as v_identification_codes,
        v:indicators                        as v_indicators,
        v:institutionTelephones             as v_institution_telephones,
        v:internationalAddresses            as v_international_addresses,
        -- references
        v:stateEducationAgencyReference     as state_education_agency_reference,
        -- edfi extensions
        v:_ext as v_ext
    from service_centers
)
select * from renamed