with leas as (
    {{ source_edfi3('local_education_agencies') }}
),
renamed as (
    select
        tenant_code,
        api_year,
        pull_timestamp,
        file_row_number,
        filename,
        is_deleted,
        v:id::string                      as record_guid,
        v:localEducationAgencyId::int     as lea_id,
        v:nameOfInstitution::string       as lea_name,
        v:shortNameOfInstitution::string  as lea_short_name,
        v:webSite::string                 as website,
        -- reference ids
        v:parentLocalEducationAgencyReference:localEducationAgencyId::int as parent_lea_id,
        v:educationServiceCenterReference:educationServiceCenterId::int   as education_service_center_id,
        v:stateEducationAgencyReference:stateEducationAgencyId::int       as state_education_agency_id,
        -- descriptors
        {{ extract_descriptor('v:operationalStatusDescriptor::string') }}            as operational_status,
        {{ extract_descriptor('v:charterStatusDescriptor::string') }}                as charter_status,
        {{ extract_descriptor('v:localEducationAgencyCategoryDescriptor::string') }} as lea_category,
        --references
        v:parentLocalEducationAgencyReference as parent_local_education_agency_reference,
        v:stateEducationAgencyReference       as state_education_agency_reference,
        v:educationServiceCenterReference     as education_service_center_reference,
        -- unflattened lists
        v:accountabilities       as v_accountabilities,
        v:addresses              as v_addresses,
        v:categories             as v_categories,
        v:federalFunds           as v_federal_funds,
        v:identificationCodes    as v_identification_codes,
        v:indicators             as v_indicators,
        v:institutionTelephones  as v_institution_telephones,
        v:internationalAddresses as v_international_addresses,

        -- edfi extensions
        v:_ext as v_ext
    from leas
)
select * from renamed