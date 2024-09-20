with base_discipline_incident as (
    select * from {{ ref('base_ef3__discipline_incidents') }}
    where not is_deleted
),
keyed as (
    select 
        {{ dbt_utils.generate_surrogate_key(
            ['tenant_code',
             'api_year',
             'lower(incident_id)',
             'school_id']
        ) }} as k_discipline_incident,
        {{ gen_skey('k_school') }},
        api_year as school_year,
        base_discipline_incident.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_discipline_incident
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_discipline_incident',
            order_by='pull_timestamp desc'
        )
    }}
)
select * from deduped
