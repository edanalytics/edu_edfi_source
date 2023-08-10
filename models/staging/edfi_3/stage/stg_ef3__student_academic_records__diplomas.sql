with stg_academic_records as (
    select * from {{ ref('stg_ef3__student_academic_records') }}
),
flattened as (
    select 
        tenant_code,
        api_year,
        k_student_academic_record,
        {{ extract_descriptor('value:diplomaTypeDescriptor::string') }} as diploma_type,
        value:diplomaAwardDate::date as diploma_award_date,
        value:diplomaDescription::string as diploma_description,
        {{ extract_descriptor('value:diplomaLevelDescriptor::string') }} as diploma_level_descriptor,
        {{ extract_descriptor('value:achievementCategoryDescriptor::string') }} as achievement_category_descriptor,
        value:achievementCategorySystem::string as achievement_category_system,
        value:achievementTitle::string as achievement_title,
        value:criteria::string as criteria,
        value:criteriaUrl::string as criteria_url,
        value:cteCompleter::boolean as is_cte_completer,
        value:diplomaAwardExpiresDate::date as diploma_award_expires_date,
        value:evidenceStatement::string as evidence_statement,
        value:imageURL::string as image_url,
        value:issuerName::string as issuer_name,
        value:issuerOriginURL::string as issuer_origin_url
    from stg_academic_records
        , lateral flatten(input=>v_diplomas)
)
select * from flattened
