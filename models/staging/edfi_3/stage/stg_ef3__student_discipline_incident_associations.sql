-- note: this model is deprecated, but still in use
with base_student_discipline_incident as (
    select * from {{ ref('base_ef3__student_discipline_incident_associations') }}
    where not is_deleted
),
keyed as (
    select 
        {{ gen_skey('k_student') }},
        {{ gen_skey('k_discipline_incident') }},
        base_student_discipline_incident.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_student_discipline_incident
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_student, k_discipline_incident',
            order_by='pull_timestamp desc'
        )
    }}
)
select * from deduped