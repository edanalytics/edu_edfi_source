-- SPDX-FileCopyrightText: 2022 Education Analytics
-- SPDX-License-Identifier: MPL-2.0

{% macro stg_post_hook_delete() %}
{% if is_incremental() %}
    DELETE FROM {{ this }} WHERE is_deleted;
{% endif %}
{% endmacro %}