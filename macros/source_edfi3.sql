-- SPDX-FileCopyrightText: 2022 Education Analytics
-- SPDX-License-Identifier: MPL-2.0

{#- Created an is_deleted flag using deletes resources -#}
{% macro source_edfi3(resource, join_deletes=True) -%}

    {% if join_deletes %}
        select
            api_data.*,
            deletes_data.deleted_id is not null as is_deleted,
            coalesce(deletes_data.pull_timestamp, api_data.pull_timestamp) as last_modified_timestamp

        from {{ source('raw_edfi_3', resource) }} as api_data

            left join (
                {{ edu_edfi_source.edfi3_deletes_for(resource) }}
            ) as deletes_data
            on (
                api_data.tenant_code = deletes_data.tenant_code
                and api_data.api_year = deletes_data.api_year
                and api_data.v:id::string = deletes_data.deleted_id
            )

    {% else %}
        select *, false as is_deleted, pull_timestamp as last_modified_timestamp
        from {{ source('raw_edfi_3', resource) }}

    {% endif %}

{%- endmacro %}


{#-
The deletes for a single resource, with the record id normalized for joining to api data.

By default, deletes for every resource land in one shared _deletes table, filtered here by name.
Resources listed in `edu:edfi_source:per_resource_deletes` instead have their deletes loaded to
their own raw table, `_deletes__<resource>` (which must be declared in the raw_edfi_3 source).
This avoids scanning every resource's deletes, which matters most for dynamic tables: each
incremental refresh otherwise reads all of _deletes to find the few rows for its resource.
-#}
{% macro edfi3_deletes_for(resource) -%}
    {%- set per_resource = resource | lower in var('edu:edfi_source:per_resource_deletes', []) -%}
    select
        tenant_code,
        api_year,
        pull_timestamp,
        replace({{ edu_edfi_source.json_ci_get('v', 'id') }}::string, '-') as deleted_id
    {% if per_resource %}
    from {{ source('raw_edfi_3', '_deletes__' ~ resource | lower) }}
    {% else %}
    from {{ source('raw_edfi_3', '_deletes') }}
    where name = '{{ resource | lower }}'
    {% endif %}
{%- endmacro %}
