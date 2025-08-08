with base_recruitment_event_attendances as (
    select * from {{ ref('base_tpdm__recruitment_event_attendances') }}
),
keyed as (
    select
        {{ dbt_utils.generate_surrogate_key(
            [
                'tenant_code',
                'api_year',
                'lower(attendee_identifier)',
                'ed_org_id',
                'event_date',
                'lower(event_title)'
            ]
        ) }} as k_recruitment_event_attendance,
        {{ gen_skey('k_recruitment_event') }},
        base_recruitment_event_attendances.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_recruitment_event_attendances
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_recruitment_event_attendance',
            order_by='last_modified_timestamp desc, pull_timestamp desc'
        )
    }}
)
select * from deduped
where not is_deleted