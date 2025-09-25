with professional_dev_event_attendances as (
    select * from {{ ref('base_tpdm__professional_development_event_attendances')}}
),
keyed as (
    select
        {{ gen_skey('k_person') }},
        {{ gen_skey('k_professional_development_event') }},
        professional_dev_event_attendances.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from professional_dev_event_attendances
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_person, k_professional_development_event, attendance_date',
            order_by='last_modified_timestamp desc, pull_timestamp desc'
        )
    }}
)
select * from deduped
where not is_deleted