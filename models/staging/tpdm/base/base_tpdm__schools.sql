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

        v:id::string                                                as record_guid,
        v:schoolId::int                                             as school_id,
        v:localEducationAgencyReference:localEducationAgencyId::int as lea_id,
        -- descriptors
        {{ extract_descriptor('v:schoolTypeDescriptor::string') }}                         as school_type,
        {{ extract_descriptor('v:postSecondaryInstitutionLevelDescriptor::string')}}       as post_secondary_institution_level,
        {{ extract_descriptor('v:administrativeFundingControlDescriptor::string')}}        as administrative_funding_control,
        {{ extract_descriptor('v:federalLocaleCodeDescriptor::string')}}                   as federal_locale_code,
        {{ extract_descriptor('v:internetAccessDescriptor::string') }}                     as internet_access,
        {{ extract_descriptor('v:charterApprovalAgencyTypeDescriptor::string') }}          as charter_approval_agency,
        {{ extract_descriptor('v:magnetSpecialProgramEmphasisSchoolDescriptor::string') }} as magnet_type,
        {{ extract_descriptor('v:titleIPartASchoolDesignationDescriptor::string') }}       as title_i_part_a_school_designation,
        {{ extract_descriptor('v:charterStatusDescriptor::string') }}                      as charter_status,
        {{ extract_descriptor('v:accreditationStatusDescriptor::string') }}                as accreditation_status,
        -- references
        v:localEducationAgencyReference   as local_education_agency_reference,
        -- unflattened lists
        v:gradeLevels          as v_grade_levels,
        v:schoolCategories     as v_school_categories,
        v:mediumOfInstructions as v_medium_of_instructions
    from student_academic_records
)

select * from renamed