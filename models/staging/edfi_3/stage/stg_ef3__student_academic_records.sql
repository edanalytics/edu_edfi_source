-- SPDX-FileCopyrightText: 2022 Education Analytics
-- SPDX-License-Identifier: MPL-2.0

{{ config(
    materialized=var('edu:edfi_source:large_stg_materialization', 'table'),
    unique_key=['k_student_academic_record', 'api_year'],
    post_hook=["{{edu_edfi_source.stg_post_hook_delete()}}"]
) }}
with base_academic_records as (
    select * from {{ ref('base_ef3__student_academic_records') }}

    {% if is_incremental() %}
    -- Only get newly added or deleted records since the last run
    where last_modified_timestamp > (select max(last_modified_timestamp) from {{ this }})
    {% endif %}
),
-- A student's 2024 academic record may appear in both the 2024 and 2025 ods.
-- Here, we keep the copy from each ods, since the newer copy may contain updates but leave out information from the older one.
-- If the record was updated or pulled multiple times from the same ods, keep only its latest version.
-- The copies share a k_student_academic_record when the tenant, student, school year, ed org, and term are unchanged.
-- api_year tells us which ods each copy came from, so the key + api_year identifies one row here.
-- keep this key definition in sync with gen_skey('k_student_academic_record', annualize=False).
keyed as (
    select
        {{ dbt_utils.generate_surrogate_key(
            [
                'tenant_code',
                'ed_org_id',
                'school_year',
                'lower(student_unique_id)',
                'lower(academic_term)'
            ]
        ) }} as k_student_academic_record,
        {{ gen_skey('k_student') }},
        {{ gen_skey('k_student_xyear') }},
        {{ edorg_ref(annualize=False) }},
        base_academic_records.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_academic_records
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_student_academic_record, api_year',
            order_by='last_modified_timestamp desc, pull_timestamp desc'
        )
    }}
)
select * from deduped
{% if not is_incremental() %}
where not is_deleted
{% endif %}

