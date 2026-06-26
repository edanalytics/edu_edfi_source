with stu_ed_org as (
    {{ source_edfi3('student_education_organization_associations') }}
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
        v:id::string as record_guid,
        ods_version,
        data_model_version,
        -- key columns
        v:educationOrganizationReference:educationOrganizationId::int as ed_org_id,
        v:educationOrganizationReference:link:rel::string             as ed_org_type,
        v:studentReference:studentUniqueId::string                    as student_unique_id,
        -- value columns
        v:hispanicLatinoEthnicity::boolean   as has_hispanic_latino_ethnicity,
        v:loginId::string                    as login_id,
        v:profileThumbnail                   as profile_thumbnail_url,
        v:genderIdentity                     as gender_identity,
        v:internetAccessInResidence::boolean as has_internet_access_in_residence,
        -- descriptors
        {{ extract_descriptor('v:limitedEnglishProficiencyDescriptor::string') }}   as lep_code,
        {{ extract_descriptor('v:sexDescriptor::string') }}                         as gender,
        {{ extract_descriptor('v:oldEthnicityDescriptor::string') }}                as old_ethnicity,
        {{ extract_descriptor('v:supporterMilitaryConnectionDescriptor::string') }} as supporter_military_connection,
        {{ extract_descriptor('v:barrierToInternetAccessInResidenceDescriptor::string') }}  as barrier_to_internet_access_in_residence,
        {{ extract_descriptor('v:internetAccessTypeInResidenceDescriptor::string') }}       as internet_access_type_in_residence,
        {{ extract_descriptor('v:internetPerformanceInResidenceDescriptor::string') }}      as internet_performance_in_residence,
        {{ extract_descriptor('v:primaryLearningDeviceAccessDescriptor::string') }}         as primary_learning_device_access,
        {{ extract_descriptor('v:primaryLearningDeviceAwayFromSchoolDescriptor::string') }} as primary_learning_device_away_from_school,
        {{ extract_descriptor('v:primaryLearningDeviceProviderDescriptor::string') }}       as primary_learning_device_provider,
        -- references
        v:studentReference               as student_reference,
        v:educationOrganizationReference as education_organization_reference,
        -- lists
        v:addresses                  as v_addresses,
        v:ancestryEthnicOrigins      as v_ancestry_ethnic_origins,
        v:cohortYears                as v_cohort_years,
        v:disabilities               as v_disabilities,
        v:displacedStudents          as v_displaced_students,
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
