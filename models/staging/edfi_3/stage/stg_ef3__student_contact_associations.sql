-- parents were renamed to contacts in Data Standard v5.0
with union_parent_contact as (
    {{
        dbt_utils.union_relations(
            relations=[
                ref('base_ef3__student_contact_associations'), 
                ref('base_ef3__student_parent_associations')
            ],
    )
    }}
),
formatted as (
    select
        *,
        -- align old and new key name
        coalesce(
            contact_reference:contactUniqueId::string,
            parent_reference:parentUniqueId::string
        ) as contact_unique_id
    from union_parent_contact
),
keyed as (
    select
        {{ gen_skey('k_student') }},
        -- we can't use the gen_skey macro here because we're bringing in the deprecated parents endpoint data, which contains a parentReference that won't work
        {{ dbt_utils.generate_surrogate_key(['tenant_code', 'lower(contact_unique_id)']) }} as k_contact,
        {{ gen_skey('k_student_xyear') }},
        api_year as school_year,
        formatted.*
        {{ extract_extension(model_name=[this.name, 'stg_ef3__student_parent_associations'], flatten=True) }}
    from formatted
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_student, k_contact',
            order_by='last_modified_timestamp desc, pull_timestamp desc'
        )
    }}
)
select * from deduped
where not is_deleted