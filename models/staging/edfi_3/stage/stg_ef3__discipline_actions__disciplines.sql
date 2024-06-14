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
        {{ extract_descriptor('value:disciplineDescriptor::string') }} as discipline_type,

        -- edfi extensions
        value:_ext as v_ext
    from stg_discipline_actions,
        lateral flatten(input => v_disciplines)
),
extended as (
    select 
        flattened.*
        {{ extract_extension(model_name=this.name, flatten=True) }}

    from flattened
)

select * from extended