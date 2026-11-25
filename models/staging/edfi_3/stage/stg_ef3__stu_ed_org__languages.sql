-- SPDX-FileCopyrightText: 2022 Education Analytics
-- SPDX-License-Identifier: MPL-2.0

-- languages moved from studentEducationOrganizationAssociations to studentDemographics after DS5.2.
-- An ODS is on one data standard, so only one side of the union has rows for a given tenant/year.
with stg_stu_ed_org as (
    {{
        dbt_utils.union_relations(
            relations=[
                ref('stg_ef3__student_education_organization_associations'),
                ref('stg_ef3__student_demographics')
            ],
            include=['tenant_code', 'api_year', 'k_student', 'k_student_xyear', 'ed_org_id', 'k_lea', 'k_school', 'v_languages']
        )
    }}
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
        {{ extract_descriptor('lang_uses.value:languageUseDescriptor::string') }} as language_use,
        {{ extract_descriptor('lang.value:languageDescriptor::string') }} as code_value
    from stg_stu_ed_org
        {{ json_flatten('v_languages', 'lang') }}
        {{ json_flatten('lang.value:uses', 'lang_uses') }}
)
select * from flattened
