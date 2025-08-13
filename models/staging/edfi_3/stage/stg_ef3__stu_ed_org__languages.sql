with stg_stu_ed_org as (
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
        {{ extract_descriptor('lang_uses.value:languageUseDescriptor::string') }} as language_use,
        {{ extract_descriptor('lang.value:languageDescriptor::string') }} as code_value
    from stg_stu_ed_org
        {{ json_flatten('v_languages', 'lang') }}
        {{ json_flatten('lang.value:uses', 'lang_uses') }}
)
select * from flattened
