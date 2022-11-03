with stu_ed_org as (
    {{ source_edfi3('student_education_organization_associations') }}
),
renamed as (
    select
        tenant_code,
        api_year,
        pull_timestamp,
        file_row_number,
        filename,
        is_deleted,
        v:id::string as record_guid,
        -- key columns
        v:educationOrganizationReference:educationOrganizationId::int as ed_org_id,
        v:educationOrganizationReference:link:rel::string             as ed_org_type,
        v:studentReference:studentUniqueId::string                    as student_unique_id,
        -- value columns
        v:hispanicLatinoEthnicity::boolean as has_hispanic_latino_ethnicity,
        v:loginId::string                  as login_id,
        v:profileThumbnail                 as profile_thumbnail_url,
        -- descriptors
        {{ extract_descriptor('v:limitedEnglishProficiencyDescriptor::string') }} as lep_code,
        {{ extract_descriptor('v:sexDescriptor::string') }}                       as gender,
        {{ extract_descriptor('v:oldEthnicityDescriptor::string') }}              as old_ethnicity,
        -- references
        v:studentReference               as student_reference,
        v:educationOrganizationReference as education_organization_reference,
        -- lists
        v:addresses                  as v_addresses,
        v:ancestryEthnicOrigins      as v_ancestry_ethnic_origins,
        v:cohortYears                as v_cohort_years,
        v:disabilities               as v_disabilities,
        v:electronicMails            as v_electronic_mails,
        v:internationalAddresses     as v_international_addresses,
        v:languages                  as v_languages,
        v:programParticipations      as v_program_participations, -- deprecated
        v:races                      as v_races,
        v:studentCharacteristics     as v_student_characteristics,
        v:studentIdentificationCodes as v_student_identification_codes,
        v:studentIndicators          as v_student_indicators,
        v:telephones                 as v_telephones,
        v:tribalAffiliations         as v_tribal_affiliations,

        -- edfi extensions
        v:_ext as v_ext
    from stu_ed_org
)
select * from renamed
