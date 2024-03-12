with stg_ef3__student_cte_program_associations as (
    select * from  {{  ref('stg_ef3__student_cte_program_associations')  }} 
),
flattened as (
    select
        tenant_code,
        api_year,
        k_student,
        k_student_xyear ,
        ed_org_id,
        k_lea,
        k_school,
        k_program,
        school_year

        ,  {{  edu_edfi_source.extract_descriptor('value:cteProgramServiceDescriptor::string')  }}  as cte_program_service
        , value:beginDate::date as begin_date
        , value:endDate::date as end_date
        
    from stg_ef3__student_cte_program_associations,
        lateral flatten(input => v_cte_program_services)
)
select * from flattened