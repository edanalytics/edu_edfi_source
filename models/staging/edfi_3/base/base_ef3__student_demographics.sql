-- SPDX-FileCopyrightText: 2022 Education Analytics
-- SPDX-License-Identifier: MPL-2.0

with stu_demos as (
    {{ source_edfi3('student_demographics') }}
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
        v:genderIdentity::string             as gender_identity,
        v:hispanicLatinoEthnicity::boolean   as has_hispanic_latino_ethnicity, -- deprecated

        -- descriptors
        {{ extract_descriptor('v:citizenshipStatusDescriptor::string') }}            as citizenship_status,
        {{ extract_descriptor('v:economicDisadvantageDescriptor::string') }}         as economic_disadvantage,
        {{ extract_descriptor('v:limitedEnglishProficiencyDescriptor::string') }}    as lep_code,
        {{ extract_descriptor('v:sexDescriptor::string') }}                          as birth_sex, --to match students, but maybe we should keep as sex?
        {{ extract_descriptor('v:supporterMilitaryConnectionDescriptor::string') }}  as supporter_military_connection,

        -- references
        v:studentReference               as student_reference,
        v:educationOrganizationReference as education_organization_reference,

        -- lists
        v:ancestryEthnicOrigins      as v_ancestry_ethnic_origins,
        v:disabilities               as v_disabilities,
        v:identificationDocuments    as v_identification_documents,
        v:languages                  as v_languages,
        v:races                      as v_races,
        v:studentCharacteristics     as v_student_characteristics,
        v:tribalAffiliations         as v_tribal_affiliations,
        v:visas                      as v_visas,

        -- edfi extensions
        v:_ext as v_ext

    from stu_demos
)
select * from renamed
