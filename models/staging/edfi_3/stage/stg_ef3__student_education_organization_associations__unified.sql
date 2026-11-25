-- SPDX-FileCopyrightText: 2022 Education Analytics
-- SPDX-License-Identifier: MPL-2.0

-- After DS5.2, most of studentEducationOrganizationAssociations was split out into studentDemographics and studentDirectories,
-- and a few fields on students (citizenship, identification documents, visas) moved to studentDemographics too.
-- This model puts them back together at the SEOA grain (one row per student per ed org)
-- so downstream models can read one place regardless of data standard.

-- An ODS is on one data standard, so for a given tenant/year only one side of each coalesce is populated:
    -- SEOA on DS5
    -- demographics/directories on DS6+
-- Full outer joins keep students who only have a demographics or directory record at an ed org
-- (possible on DS6+, where SEOA is no longer required for these).

{#-
    The select below builds the keys and moved fields by hand, coalescing each one across SEOA and demographics/directories. 
    It then uses star() to pull in whatever is left on SEOA. 
    These lists tell star() which SEOA columns to skip, 
    either because the hand-built coalesce already covers them or because they should be dropped.
-#}
{%- set key_cols = [
    'tenant_code', 'api_year', 'school_year',
    'k_student', 'k_student_xyear', 'k_lea', 'k_school',
    'ed_org_id', 'ed_org_type', 'student_unique_id',
    'student_reference', 'education_organization_reference'
] -%}
{%- set moved_to_demographics = [
    'has_hispanic_latino_ethnicity', 'gender_identity', 'lep_code', 'gender', 'supporter_military_connection',
    'v_ancestry_ethnic_origins', 'v_disabilities', 'v_languages', 'v_races',
    'v_student_characteristics', 'v_tribal_affiliations'
] -%}
{%- set moved_to_directories = [
    'v_addresses', 'v_electronic_mails', 'v_international_addresses', 'v_telephones'
] -%}
{#- dropped: a unified row can come from up to three source records, so no single value is right -#}
{%- set record_metadata = [
    'pull_timestamp', 'last_modified_timestamp', 'file_row_number', 'filename',
    'is_deleted', 'record_guid', 'ods_version', 'data_model_version'
] %}

with stu_ed_org as (
    select * from {{ ref('stg_ef3__student_education_organization_associations') }}
),
stu_demographics as (
    select * from {{ ref('stg_ef3__student_demographics') }}
),
stu_directories as (
    select * from {{ ref('stg_ef3__student_directories') }}
),
-- only the fields that moved from students to studentDemographics
students as (
    select
        k_student,
        citizenship_status,
        v_identification_documents,
        v_visas
    from {{ ref('stg_ef3__students') }}
),
joined as (
    select
        -- keys
        coalesce(stu_ed_org.tenant_code,       stu_demographics.tenant_code,       stu_directories.tenant_code)       as tenant_code,
        coalesce(stu_ed_org.api_year,          stu_demographics.api_year,          stu_directories.api_year)          as api_year,
        coalesce(stu_ed_org.school_year,       stu_demographics.school_year,       stu_directories.school_year)       as school_year,
        coalesce(stu_ed_org.k_student,         stu_demographics.k_student,         stu_directories.k_student)         as k_student,
        coalesce(stu_ed_org.k_student_xyear,   stu_demographics.k_student_xyear,   stu_directories.k_student_xyear)   as k_student_xyear,
        coalesce(stu_ed_org.k_lea,             stu_demographics.k_lea,             stu_directories.k_lea)             as k_lea,
        coalesce(stu_ed_org.k_school,          stu_demographics.k_school,          stu_directories.k_school)          as k_school,
        coalesce(stu_ed_org.ed_org_id,         stu_demographics.ed_org_id,         stu_directories.ed_org_id)         as ed_org_id,
        coalesce(stu_ed_org.ed_org_type,       stu_demographics.ed_org_type,       stu_directories.ed_org_type)       as ed_org_type,
        coalesce(stu_ed_org.student_unique_id, stu_demographics.student_unique_id, stu_directories.student_unique_id) as student_unique_id,
        coalesce(stu_ed_org.student_reference, stu_demographics.student_reference, stu_directories.student_reference) as student_reference,
        coalesce(
            stu_ed_org.education_organization_reference,
            stu_demographics.education_organization_reference,
            stu_directories.education_organization_reference
        ) as education_organization_reference,

        -- moved to studentDemographics after DS5.2
        coalesce(stu_ed_org.has_hispanic_latino_ethnicity, stu_demographics.has_hispanic_latino_ethnicity) as has_hispanic_latino_ethnicity,
        coalesce(stu_ed_org.gender_identity,               stu_demographics.gender_identity)               as gender_identity,
        coalesce(stu_ed_org.lep_code,                      stu_demographics.lep_code)                      as lep_code,
        coalesce(stu_ed_org.gender,                        stu_demographics.birth_sex)                     as gender,
        coalesce(stu_ed_org.supporter_military_connection, stu_demographics.supporter_military_connection) as supporter_military_connection,
        coalesce(stu_ed_org.v_ancestry_ethnic_origins,     stu_demographics.v_ancestry_ethnic_origins)     as v_ancestry_ethnic_origins,
        coalesce(stu_ed_org.v_disabilities,                stu_demographics.v_disabilities)                as v_disabilities,
        coalesce(stu_ed_org.v_languages,                   stu_demographics.v_languages)                   as v_languages,
        coalesce(stu_ed_org.v_races,                       stu_demographics.v_races)                       as v_races,
        coalesce(stu_ed_org.v_student_characteristics,     stu_demographics.v_student_characteristics)     as v_student_characteristics,
        coalesce(stu_ed_org.v_tribal_affiliations,         stu_demographics.v_tribal_affiliations)         as v_tribal_affiliations,

        -- moved from students to studentDemographics after DS5.2. On DS5.2 the single student-level
        -- value is repeated on each of the student's ed org rows; on DS6+ each ed org has its own.
        coalesce(students.citizenship_status,         stu_demographics.citizenship_status)         as citizenship_status,
        coalesce(students.v_identification_documents, stu_demographics.v_identification_documents) as v_identification_documents,
        coalesce(students.v_visas,                    stu_demographics.v_visas)                    as v_visas,

        -- new on studentDemographics, no DS5.2 equivalent
        stu_demographics.economic_disadvantage,

        -- moved to studentDirectories after DS5.2
        coalesce(stu_ed_org.v_addresses,               stu_directories.v_addresses)               as v_addresses,
        coalesce(stu_ed_org.v_electronic_mails,        stu_directories.v_electronic_mails)        as v_electronic_mails,
        coalesce(stu_ed_org.v_international_addresses, stu_directories.v_international_addresses) as v_international_addresses,
        coalesce(stu_ed_org.v_telephones,              stu_directories.v_telephones)              as v_telephones,

        -- everything else on SEOA: fields that stayed on SEOA, DS5.2-only fields with no new home
        -- (old_ethnicity, v_program_participations, v_student_identification_codes), v_ext, and any
        -- configured extension columns
        {{ edu_edfi_source.star('stu_ed_org',
            except=key_cols + moved_to_demographics + moved_to_directories + record_metadata
        ) }}

    from stu_ed_org
    full outer join stu_demographics
        on  stu_ed_org.k_student = stu_demographics.k_student
        and stu_ed_org.ed_org_id = stu_demographics.ed_org_id
    full outer join stu_directories
        on  coalesce(stu_ed_org.k_student, stu_demographics.k_student) = stu_directories.k_student
        and coalesce(stu_ed_org.ed_org_id, stu_demographics.ed_org_id) = stu_directories.ed_org_id
    -- students is one row per k_student, so this can't fan out rows
    left join students
        on coalesce(stu_ed_org.k_student, stu_demographics.k_student, stu_directories.k_student) = students.k_student
)
select * from joined
