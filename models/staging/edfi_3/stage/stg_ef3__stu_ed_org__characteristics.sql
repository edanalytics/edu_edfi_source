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
        {{ extract_descriptor('char.value:studentCharacteristicDescriptor::string') }} as student_characteristic,
        char.value:designatedBy::string as designated_by,
        timing.value:beginDate::date as begin_date,
        timing.value:endDate::date as end_date
    from stage_stu_ed_org
        {{ json_flatten('v_student_characteristics', 'char', outer=True) }}
        {{ json_flatten('char.value:periods', 'timing', outer=True) }}
)
select * from flattened

