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
        ind.value:indicatorName::string  as indicator_name,
        ind.value:indicator::string      as indicator_value,
        ind.value:indicatorGroup::string as indicator_group, 
        ind.value:designatedBy::string   as designated_by,
        timing.value:beginDate::date     as begin_date,
        timing.value:endDate::date       as end_date
    from stage_stu_ed_org
        , lateral flatten(input=>v_student_indicators) as ind
        , lateral flatten(input=>ind.value:periods, outer=>true) as timing
)
select * from flattened

