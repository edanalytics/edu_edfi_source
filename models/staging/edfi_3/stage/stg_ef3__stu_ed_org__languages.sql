with stg_stu_ed_org as (
    select * from {{ ref('stg_ef3__student_education_organization_associations') }}
),
stg_descriptors as (
    select * from {{ ref('stg_ef3__descriptors')}}
    where descriptor_name = 'language_descriptors'
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
),
-- pick home language if exists
choose_language_use as (
    {{ row_pluck('flattened',
                key='k_student',
                column='language_use',
                preferred='Home language') }}
)
select 
    distinct
        ft.tenant_code,
        ft.api_year,
        ft.k_student,
        ft.k_student_xyear,
        ft.ed_org_id,
        ft.k_lea,
        ft.k_school,
        descr.short_description as language_name, 
        choose_language_use.language_use
from flattened ft
left join choose_language_use on ft.k_student = choose_language_use.k_student
left join stg_descriptors descr on  ft.tenant_code = descr.tenant_code 
                                and ft.api_year   = descr.api_year
                                and ft.code_value = descr.code_value