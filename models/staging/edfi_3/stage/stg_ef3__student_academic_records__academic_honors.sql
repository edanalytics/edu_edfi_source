with stg_academic_records as (
    select * from  {{  ref('stg_ef3__student_academic_records')  }} 
),
flattened as (
    select 
        tenant_code,
        api_year,
        k_student_academic_record,
        {{  extract_descriptor('value:academicHonorCategoryDescriptor::string')  }}  as academic_honor_category_code,
        value:"honorDescription"::string as honor_description,
        value:"honorAwardDate"::date as honor_award_date,
        value:"honorAwardExpiresDate"::date as honor_award_expires_date,
        {{ extract_descriptor('value:achievementCategoryDescriptor::string') }} as achievement_category_descriptor,
        value:achievementCategorySystem::string as achievement_category_system,
        value:achievementTitle::string as achievement_title,
        value:criteria::string as criteria,
        value:criteriaUrl::string as criteria_url,
        value:evidenceStatement::string as evidence_statement,
        value:imageURL::string as image_url,
        value:issuerName::string as issuer_name,
        value:issuerOriginURL::string as issuer_origin_url,
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