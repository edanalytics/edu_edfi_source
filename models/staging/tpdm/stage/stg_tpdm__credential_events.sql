with base_credential_events as (
    select * from {{ ref('base_tpdm__credential_events')}}
),
keyed as (
    select
        {{ gen_skey('k_credential') }},
        base_credential_events.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_credential_events
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_credential, credential_event_date, credential_event_type, credential_identifier',
            order_by='last_modified_timestamp desc, pull_timestamp desc'
        )
    }}
)
select * from deduped
where not is_deleted