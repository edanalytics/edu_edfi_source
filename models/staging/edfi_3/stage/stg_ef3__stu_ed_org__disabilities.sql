-- SPDX-FileCopyrightText: 2022 Education Analytics
-- SPDX-License-Identifier: MPL-2.0

-- disabilities moved from studentEducationOrganizationAssociations to studentDemographics after DS5.2.
-- An ODS is on one data standard, so only one side of the union has rows for a given tenant/year.
with stg_stu_ed_org as (
    {{
        dbt_utils.union_relations(
            relations=[
                ref('stg_ef3__student_education_organization_associations'),
                ref('stg_ef3__student_demographics')
            ],
            include=['tenant_code', 'api_year', 'school_year', 'k_student', 'k_student_xyear', 'ed_org_id', 'k_lea', 'k_school', 'v_disabilities']
        )
    }}
),
flattened as (
    select
        tenant_code,
        api_year,
        school_year,
        k_student,
        k_student_xyear,
        ed_org_id,
        k_lea,
        k_school,
        {{ extract_descriptor('disab.value:disabilityDescriptor::string') }} as disability_type,
        {{ extract_descriptor('disab.value:disabilityDeterminationSourceTypeDescriptor::string') }} as disability_source_type,
        disab.value:disabilityDiagnosis::string as disability_diagnosis,
        disab.value:orderOfDisability::int as order_of_disability,
        disab.value:designations as v_designations
    from stg_stu_ed_org
        {{ json_flatten('v_disabilities', 'disab') }}
)
select * from flattened
