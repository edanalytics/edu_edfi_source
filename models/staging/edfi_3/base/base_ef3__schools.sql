with schools as (
    {{ source_edfi3('schools') }}
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
        v:schoolId::integer              as school_id,
        v:nameOfInstitution::string      as school_name,
        v:shortNameOfInstitution::string as school_short_name,
        v:webSite::string                as website,
        v:localEducationAgencyReference:localEducationAgencyId as lea_id,
        -- pull out school categories
        case
            when array_size(v:schoolCategories) = 1
                then {{ extract_descriptor('v:schoolCategories[0]:schoolCategoryDescriptor::string') }}
            when array_size(v:schoolCategories) > 1
                then 'Multiple Categories'
            else NULL
        end as school_category,
        -- descriptors
        {{ extract_descriptor('v:schoolTypeDescriptor::string') }}                         as school_type,
        {{ extract_descriptor('v:operationalStatusDescriptor::string') }}                  as operational_status,
        {{ extract_descriptor('v:administrativeFundingControlDescriptor::string') }}       as administrative_funding_control,
        {{ extract_descriptor('v:internetAccessDescriptor::string') }}                     as internet_access,
        {{ extract_descriptor('v:titleIPartASchoolDesignationDescriptor::string') }}       as title_i_part_a_school_designation,
        {{ extract_descriptor('v:charterStatusDescriptor::string') }}                      as charter_status,
        {{ extract_descriptor('v:charterApprovalAgencyTypeDescriptor::string') }}          as charter_approval_agency,
        {{ extract_descriptor('v:magnetSpecialProgramEmphasisSchoolDescriptor::string') }} as magnet_type,
        -- references
        v:localEducationAgencyReference   as local_education_agency_reference,
        -- unflattened lists
        v:addresses                       as v_addresses,
        v:educationOrganizationCategories as v_education_organization_categories,
        v:gradeLevels                     as v_grade_levels,
        v:identificationCodes             as v_identification_codes,
        v:indicators                      as v_indicators,
        v:institutionTelephones           as v_institution_telephones,
        v:internationalAddresses          as v_international_addresses,
        v:schoolCategories                as v_school_categories,

        -- edfi extensions
        v:_ext as v_ext
    from schools
)
select * from renamed
