with education_organizations as (
    {{ source_edfi3('education_organizations') }}
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

        v:id::string                     as record_guid,
        v:educationOrganizationId:int    as education_organization_id,
        v:nameOfInstitution::string      as name_of_institution,
        v:shortNameOfInstitution::string as short_name_of_institution,
        v:webSite::string                as web_site,
        -- descriptors
        {{ extract_descriptor('v:operationalStatusDescriptor::string') }} as operational_tatus,
        -- unnested lists
        v:identificationCodes as identification_codes,
        v:categories as categories,
        v:addresses as addresses,
        v:internationalAddresses as international_addresses,
        v:institutionTelephones as institution_telephones,
        v:indicators as indicators
    from education_organization
)

select * from renamed
