{% macro stg_post_hook_delete() %}
{% if is_incremental() %}
    DELETE FROM {{ this }} WHERE is_deleted;
{% endif %}
{% endmacro %}