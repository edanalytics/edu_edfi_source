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
        , lateral flatten(input=>v_languages) as lang
        , lateral flatten(input=>lang.value:uses) as lang_uses
)
select * from flattened