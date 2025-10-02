with stg_academic_records as (
    select * from  {{  ref('stg_ef3__student_academic_records')  }} 
),
flattened as (
    select 
        tenant_code,
        api_year,
        k_student_academic_record,
        k_student,
        k_student_xyear,
        k_lea,
        k_school,
        {{  extract_descriptor('value:academicHonorCategoryDescriptor::string')  }}  as academic_honor_category_code,
        value:"honorDescription"::string as honor_description,
        try_to_date(value:"honorAwardDate"::date) as honor_award_date
         -- edfi extensions
        value:_ext as v_ext 
    from stg_academic_records,
        {{ json_flatten('v_academic_honors') }}
)
-- pull out extensions from v_academic_honors.v_ext to their own columns
extended as (
    select
        flattened.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from flattened
)
select * from extended