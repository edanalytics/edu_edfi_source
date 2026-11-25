-- SPDX-FileCopyrightText: 2022 Education Analytics
-- SPDX-License-Identifier: MPL-2.0

-- studentCharacteristics moved from studentEducationOrganizationAssociations to studentDemographics after DS5.2.
-- An ODS is on one data standard, so only one side of the union has rows for a given tenant/year.
with stage_stu_ed_org as (
    {{
        dbt_utils.union_relations(
            relations=[
                ref('stg_ef3__student_education_organization_associations'),
                ref('stg_ef3__student_demographics')
            ],
            include=['tenant_code', 'api_year', 'k_student', 'k_student_xyear', 'ed_org_id', 'k_lea', 'k_school', 'v_student_characteristics']
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
        {{ extract_descriptor('char.value:studentCharacteristicDescriptor::string') }} as student_characteristic,
        char.value:designatedBy::string as designated_by,
        timing.value:beginDate::date as begin_date,
        timing.value:endDate::date as end_date
    from stage_stu_ed_org
        {{ json_flatten('v_student_characteristics', 'char') }}
        {{ json_flatten('char.value:periods', 'timing', outer=True) }}
)
select * from flattened

