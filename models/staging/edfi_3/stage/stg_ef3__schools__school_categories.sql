with stage_schools as (
    select * from {{ ref('stg_ef3__schools') }}
),
flattened as (
    select 
        tenant_code,
        api_year,
        k_school,
        {{ extract_descriptor('value:schoolCategoryDescriptor::string') }} as school_category
    from stage_schools
        , lateral flatten(input=>v_school_categories)
)
select * from flattened