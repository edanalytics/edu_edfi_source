-- SPDX-FileCopyrightText: 2022 Education Analytics
-- SPDX-License-Identifier: MPL-2.0

-- studentIdentificationCodes moved from a list on studentEducationOrganizationAssociations to its own
-- resource after DS5.2. An ODS is on one data standard, so only one side of the union has rows for a
-- given tenant/year.
-- union_relations can't take the flattened CTE, so both sides select from one shared column list
-- to keep the positional union aligned.
{% set output_cols = [
    'tenant_code', 'api_year', 'k_student', 'k_student_xyear', 'ed_org_id', 'k_lea', 'k_school',
    'id_system', 'assigning_org', 'id_code'
] %}

with stage_stu_ed_org as (
    select * from {{ ref('stg_ef3__student_education_organization_associations') }}
),
flattened as (
    select
        tenant_code,
        api_year,
        k_student,
        k_student_xyear,
        ed_org_id,
        k_lea,
        k_school,
        {{ extract_descriptor('value:studentIdentificationSystemDescriptor::string') }} as id_system,
        value:assigningOrganizationIdentificationCode::string as assigning_org,
        value:identificationCode::string as id_code
    from stage_stu_ed_org
        {{ json_flatten('v_student_identification_codes') }}
),
stu_id_codes as (
    select * from {{ ref('stg_ef3__student_identification_codes') }}
)
select {{ output_cols | join(', ') }} from flattened
union all
select {{ output_cols | join(', ') }} from stu_id_codes
