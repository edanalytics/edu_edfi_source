with stage_stu_ed_org as (
    select * from {{ ref('stg_ef3__student_education_organization_associations') }}
),
flattened as (
    select
        stage_stu_ed_org.tenant_code,
        stage_stu_ed_org.api_year,
        stage_stu_ed_org.k_student,
        stage_stu_ed_org.k_student_xyear,
        stage_stu_ed_org.ed_org_id,
        stage_stu_ed_org.k_lea,
        stage_stu_ed_org.k_school,
        ind.value:indicatorName::string  as indicator_name,
        ind.value:indicator::string      as indicator_value,
        ind.value:indicatorGroup::string as indicator_group, 
        ind.value:designatedBy::string   as designated_by,
        timing.value:beginDate::date     as begin_date,
        timing.value:endDate::date       as end_date
    from stage_stu_ed_org
        {{ json_flatten('v_student_indicators', 'ind') }}
        {{ json_flatten('ind.value:periods', 'timing', outer=True) }}
)
select * from flattened
