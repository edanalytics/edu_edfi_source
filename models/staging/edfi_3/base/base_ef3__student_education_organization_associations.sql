-- SPDX-FileCopyrightText: 2022 Education Analytics
-- SPDX-License-Identifier: MPL-2.0

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
        v:loginId::string                    as login_id,
        v:profileThumbnail                   as profile_thumbnail_url,
        v:internetAccessInResidence::boolean as has_internet_access_in_residence,
        v:hispanicLatinoEthnicity::boolean   as has_hispanic_latino_ethnicity, -- deprecated after DS5.2
        v:genderIdentity                     as gender_identity,               -- deprecated after DS5.2
        -- descriptors
        {{ extract_descriptor('v:barrierToInternetAccessInResidenceDescriptor::string') }}  as barrier_to_internet_access_in_residence,
        {{ extract_descriptor('v:internetAccessTypeInResidenceDescriptor::string') }}       as internet_access_type_in_residence,
        {{ extract_descriptor('v:internetPerformanceInResidenceDescriptor::string') }}      as internet_performance_in_residence,
        {{ extract_descriptor('v:primaryLearningDeviceAccessDescriptor::string') }}         as primary_learning_device_access,
        {{ extract_descriptor('v:primaryLearningDeviceAwayFromSchoolDescriptor::string') }} as primary_learning_device_away_from_school,
        {{ extract_descriptor('v:primaryLearningDeviceProviderDescriptor::string') }}       as primary_learning_device_provider,
        {{ extract_descriptor('v:limitedEnglishProficiencyDescriptor::string') }}   as lep_code,                      -- deprecated after DS5.2
        {{ extract_descriptor('v:sexDescriptor::string') }}                         as gender,                        -- deprecated after DS5.2
        {{ extract_descriptor('v:oldEthnicityDescriptor::string') }}                as old_ethnicity,                 -- deprecated after DS5.2
        {{ extract_descriptor('v:supporterMilitaryConnectionDescriptor::string') }} as supporter_military_connection, -- deprecated after DS5.2
        -- references
        v:studentReference               as student_reference,
        v:educationOrganizationReference as education_organization_reference,
        -- lists
        v:cohortYears                as v_cohort_years,
        v:displacedStudents          as v_displaced_students,
        v:studentIndicators          as v_student_indicators,
        v:addresses                  as v_addresses,                    -- deprecated after DS5.2
        v:ancestryEthnicOrigins      as v_ancestry_ethnic_origins,      -- deprecated after DS5.2
        v:disabilities               as v_disabilities,                 -- deprecated after DS5.2
        v:electronicMails            as v_electronic_mails,             -- deprecated after DS5.2
        v:internationalAddresses     as v_international_addresses,      -- deprecated after DS5.2
        v:languages                  as v_languages,                    -- deprecated after DS5.2
        v:programParticipations      as v_program_participations,       -- deprecated after DS5.2
        v:races                      as v_races,                        -- deprecated after DS5.2
        v:studentCharacteristics     as v_student_characteristics,      -- deprecated after DS5.2
        v:studentIdentificationCodes as v_student_identification_codes, -- deprecated after DS5.2
        v:telephones                 as v_telephones,                   -- deprecated after DS5.2
        v:tribalAffiliations         as v_tribal_affiliations,          -- deprecated after DS5.2
        -- edfi extensions
        v:_ext as v_ext
    from stu_ed_org
)
select * from renamed
