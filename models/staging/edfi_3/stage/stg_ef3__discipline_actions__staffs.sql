with stg_discipline_actions as (
    select * from {{ ref('stg_ef3__discipline_actions') }}
),
flattened as (
    select 
        tenant_code,
        api_year,
        discipline_action_id,
        discipline_date,
        k_student,
        k_student_xyear,
        {{ gen_skey('k_staff', alt_ref='value:staffReference') }},
        value:staffReference:staffUniqueId::string as staff_unique_id
    from stg_discipline_actions,
        lateral flatten(input => v_staffs)
)
select * from flattened