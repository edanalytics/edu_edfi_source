with base_sessions as (
    select * from {{ ref('base_ef3__sessions') }}
),
keyed as (
    select 
        {{ dbt_utils.generate_surrogate_key(
            [
                'tenant_code',
                'school_id',
                'school_year',
                'lower(session_name)'
            ]
        ) }} as k_session,
        {{ gen_skey('k_school') }},
        base_sessions.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_sessions
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_session',
            order_by='last_modified_timestamp desc'
        )
    }}
)
select * from deduped
where not is_deleted
order by tenant_code, school_year desc, school_id
