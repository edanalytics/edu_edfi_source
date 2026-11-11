#!/usr/bin/env bash

# Usage: lint_dbt.sh <dialect>   where dialect is `databricks` or `snowflake`.

set -euo pipefail

# Which warehouse are we pretending to be: databricks or snowflake?
dialect="${1:-}"
case "$dialect" in
  databricks|snowflake) ;;
  *) echo "usage: $0 <databricks|snowflake>" >&2; exit 2 ;;
esac

# Read the project's name so we can build a fake connection for it below.
profile=$(grep "^profile:" dbt_project.yml | awk '{print $2}' | tr -d "'\"")
# Keep each dialect's compiled output in its own folder so they don't overwrite each other.
target_path="target/lint-${dialect}"
profiles_dir="${target_path}/dummy_profile"
mkdir -p "$profiles_dir"

# per-dialect dummy profile.
if [[ "$dialect" == "databricks" ]]; then
  cat > "$profiles_dir/profiles.yml" << EOF
$profile:
  target: dry_run
  outputs:
    dry_run:
      type: databricks
      host: 127.0.0.1
      http_path: /sql/1.0/warehouses/dummy
      token: dummy
      schema: dummy
      catalog: dummy
      threads: 8
      connect_timeout: 1
      connect_retries: 0
      retry_all: false
      connection_parameters:
        _retry_stop_after_attempts_count: 1
        _socket_timeout: 3
EOF
else
  cat > "$profiles_dir/profiles.yml" << EOF
$profile:
  target: dry_run
  outputs:
    dry_run:
      type: snowflake
      account: dummy-account
      user: dummy
      password: dummy
      role: dummy
      database: dummy
      warehouse: dummy
      schema: dummy
      threads: 8
      connect_retries: 0
      connect_timeout: 3
      retry_all: false
EOF
fi

# Each of these normally hits the real warehouse during compile, so replace
# them with fake versions that just return an empty result:
#   get_filtered_columns_in_relation / unpivot -> list a real table's columns
#   get_column_values                          -> select distinct values
#   get_intervals_between                      -> a real date-diff query
#   is_incremental                             -> checks if the table exists
#   union_relations                            -> also lists a table's columns
cat > macros/_ci_lint_stub.sql << EOF
{% macro ${dialect}__get_filtered_columns_in_relation(from, except=[]) %}
  {{ return([]) }}
{% endmacro %}

{% macro ${dialect}__union_relations(relations, column_override=none, include=[], exclude=[], source_column_name='_dbt_source_relation', where=none) %}
  {{ return('') }}
{% endmacro %}

{% macro ${dialect}__get_column_values(table, column, order_by='count(*) desc', max_records=none, default=none, where=none) %}
  {{ return(default if default is not none else []) }}
{% endmacro %}

{% macro is_incremental() %}
  {{ return(False) }}
{% endmacro %}

{% macro ${dialect}__get_intervals_between(start_date, end_date, datepart) %}
  {{ return(1) }}
{% endmacro %}

{% macro ${dialect}__unpivot(relation=none, cast_to='varchar', exclude=none, remove=none, field_name='field_name', value_name='value', quote_identifiers=False) %}
  select
    {% for col in (exclude or []) %}
    cast(null as {{ cast_to }}) as {{ col }},
    {% endfor %}
    cast(null as {{ cast_to }}) as {{ field_name }},
    cast(null as {{ cast_to }}) as {{ value_name }}
  where false
{% endmacro %}

{#- edu_edfi_source's databricks__json_flatten emits "lateral variant_explode(...)",
but sqlfluff's databricks dialect can't parse this for some reason, even though it
is an actual Databricks call. When a where clause follows directly after this variant_explode,
sqlfluff errors. Adding trailing comma to force sqlfluff to look at this line instead of the
where clause. This redefines the same macro as macros/json_flatten.sql below, which is fine
since that file only keeps the dispatch + snowflake__ versions #}
{% macro databricks__json_flatten(column, alias, outer) -%}
, lateral variant_explode{% if outer %}_outer{% endif %}({{ column }}) {% if alias != '' %} as {{ alias }} {% endif %}, -- noqa: PRS
{%- endmacro %}

EOF

# json_flatten's databricks__ implementation is stubbed above (to work around
# the sqlfluff parse issue), so drop it here to avoid a duplicate macro
# definition; keep the dispatch macro and the snowflake__ implementation.
cat > macros/json_flatten.sql << 'EOF'
{% macro json_flatten(column, alias='', outer=False) %}
    {{ return(adapter.dispatch('json_flatten', 'edu_edfi_source')(column, alias, outer)) }}
{% endmacro %}

{% macro snowflake__json_flatten(column, alias, outer) -%}
, lateral flatten(input=>{{ column }}, outer=>{{ outer }}) {% if alias != '' %} as {{ alias }} {% endif %}
{%- endmacro %}
EOF

# extract_descriptor also runs a real query against the warehouse (via
# run_query), which can't use a dummy the same way as above. So replace its
# whole file with a simple version that skips the query and just returns
# the raw descriptor code.
cat > macros/extract_descriptor.sql << 'EOF'
{% macro extract_descriptor(col, descriptor_name=None) -%}
  split_part({{ col }}, '#', -1)
{%- endmacro %}
EOF

# Generate a placeholder covering every resource name actually referenced,
# with a fake database/schema, so source() resolves without needing a real one.
resources=$(grep -rhoE "source_edfi3\(\s*['\"][a-zA-Z0-9_]+['\"]" \
    models macros 2>/dev/null \
    | sed -E "s/source_edfi3\(\s*['\"]//; s/['\"]//" | sort -u)

{
  echo "sources:"
  echo "  - name: raw_edfi_3"
  echo "    database: dummy"
  echo "    schema: dummy_raw"
  echo "    tables:"
  echo "      - name: _deletes"
  for r in $resources; do
    echo "      - name: $r"
  done
} > models/_ci_lint_sources.yml

# Generate an empty placeholder seed for every ref() that isn't defined
# anywhere else, so ref() resolves without needing the real crosswalk data.
mkdir -p seeds
{
  find . -iname "*.sql" -path "*/models/*" 2>/dev/null | sed -E 's#.*/##; s/\.sql$//'
  find . -iname "*.csv" -path "*/seeds/*" 2>/dev/null | sed -E 's#.*/##; s/\.csv$//'
} | sort -u > /tmp/_ci_defined_nodes.txt

grep -rhoE "ref\(['\"][a-zA-Z0-9_]+['\"]" . --include="*.sql" 2>/dev/null \
  | sed -E "s/ref\(['\"]//; s/['\"]//" | sort -u > /tmp/_ci_all_refs.txt

for missing in $(comm -23 /tmp/_ci_all_refs.txt /tmp/_ci_defined_nodes.txt); do
  printf 'placeholder\nx\n' > "seeds/${missing}.csv"
done

# Some models talk to the warehouse directly for real data or column info,
# so they can't compile against a fake connection. Any OTHER kind of failure (a real bug, a missing
# config) stops the script.
#
# tpdm, tpdmcommunity, and sedm staging models are disabled by default.
# turn them on so they get linted too.
models_needing_warehouse=()
tests_needing_warehouse=()
compile_log=$(mktemp)
trap 'rm -f "$compile_log"' EXIT

while true; do
  exclude_flags=()
  all_excluded=("${models_needing_warehouse[@]}" "${tests_needing_warehouse[@]}")
  [[ ${#all_excluded[@]} -gt 0 ]] && exclude_flags=(--exclude "${all_excluded[@]}")

  dbt compile --no-introspect --no-populate-cache \
    --select package:edu_edfi_source \
    "${exclude_flags[@]}" \
    --vars '{"src:domain:tpdm:enabled": true, "src:domain:tpdmcommunity:enabled": true, "src:domain:sedm:enabled": true}' \
    --profiles-dir "$profiles_dir" --target dry_run --target-path "$target_path" \
    > "$compile_log" 2>&1 && break

  # dbt's error looks like "Runtime Error in model some_model_name (path/to/file.sql)"
  # or "...in test some_test_name (...)". Pull out both the node type and its name.
  culprit=""
  culprit_type=""
  grep -q "connection never acquired for thread" "$compile_log" \
    && culprit_type=$(grep -oE "Runtime Error in (model|test)" "$compile_log" | head -1 | awk '{print $NF}') \
    && culprit=$(grep -oE "Runtime Error in (model|test) [a-zA-Z0-9_]+" "$compile_log" | head -1 | awk '{print $NF}')

  if [[ -z "$culprit" ]] || [[ " ${all_excluded[*]} " == *" $culprit "* ]]; then
    echo "❌ dbt compile failed:"
    cat "$compile_log"
    exit 1
  fi

  echo "⚠️ $culprit needs a live warehouse connection to compile, adding $culprit to the exclude list and recompiling"
  if [[ "$culprit_type" == "test" ]]; then
    tests_needing_warehouse+=("$culprit")
  else
    models_needing_warehouse+=("$culprit")
  fi
done

# sqlfluff only catches syntax that doesn't parse, it can't tell that a
# function or type doesn't exist in that dialect, so check for
# these known Databricks gaps by name instead. Add new ones here as they come up.
databricks_incompatible_patterns=(
  'try_to_date\s*\('  # function has no Databricks equivalent
  '\bas\s+time\b'     # Databricks has no TIME datatype
)
incompatible_regex=$(IFS='|'; echo "${databricks_incompatible_patterns[*]}")

# Lint each compiled model/test. Generic tests (auto-generated from
# schema .yml files, like unique/not_null) compile into a "<node>.yml/"
mapfile -d '' -t compiled_files < <(find "$target_path/compiled" -path "*/edu_edfi_source/models/*" -name "*.sql" -print0)
model_total=0; model_pass=0; model_fail=0; model_failed=()
test_total=0; test_pass=0; test_fail=0; test_failed=()
for f in "${compiled_files[@]}"; do
  name=$(basename "$f")
  path=${f#*/edu_edfi_source/}
  is_test=false
  [[ "$path" == *.yml/* ]] && is_test=true

  if out=$(sqlfluff lint --config .sqlfluff --templater raw --dialect "$dialect" "$f" 2>&1); then
    sqlfluff_ok=0
  else
    sqlfluff_ok=1
  fi

  bad_function=""
  if [[ "$dialect" == "databricks" ]]; then
    match=$(grep -inE "$incompatible_regex" "$f" | head -1 || true)
    [[ -n "$match" ]] && bad_function="No Databricks equivalent: $match"
  fi

  if [[ $sqlfluff_ok -eq 0 && -z "$bad_function" ]]; then
    echo "Linting $path ✅"
    if $is_test; then test_pass=$((test_pass + 1)); else model_pass=$((model_pass + 1)); fi
  else
    printf '::group::Linting %s ❌\n' "$path"
    [[ -n "$bad_function" ]] && echo "$bad_function"
    echo "$out"
    echo "::endgroup::"
    if $is_test; then
      test_fail=$((test_fail + 1)); test_failed+=("${name%.sql}")
    else
      model_fail=$((model_fail + 1)); model_failed+=("${name%.sql}")
    fi
  fi
  if $is_test; then test_total=$((test_total + 1)); else model_total=$((model_total + 1)); fi
done

echo ""
model_known=$((model_total + ${#models_needing_warehouse[@]}))
printf '✅ %d/%d models compatible with %s\n' "$model_pass" "$model_known" "$dialect"
if [[ ${#models_needing_warehouse[@]} -gt 0 ]]; then
  printf '⚠️ %d of those %d cannot be compiled here and requires live warehouse, lint these models locally instead:\n' "${#models_needing_warehouse[@]}" "$model_known"
  printf '    - %s\n' "${models_needing_warehouse[@]}"
fi

echo ""
test_known=$((test_total + ${#tests_needing_warehouse[@]}))
printf '✅ %d/%d tests compatible with %s\n' "$test_pass" "$test_known" "$dialect"
if [[ ${#tests_needing_warehouse[@]} -gt 0 ]]; then
  printf '⚠️ %d of those %d cannot be compiled here and requires live warehouse, lint these tests locally instead:\n' "${#tests_needing_warehouse[@]}" "$test_known"
  printf '    - %s\n' "${tests_needing_warehouse[@]}"
fi
if [[ ${#test_failed[@]} -gt 0 ]]; then
  printf '❌ %d tests are NOT compatible with %s:\n' "${#test_failed[@]}" "$dialect"
  printf '    - %s\n' "${test_failed[@]}"
fi

if [[ ${#model_failed[@]} -eq 0 && ${#test_failed[@]} -eq 0 ]]; then
  exit 0
fi
echo ""
if [[ ${#model_failed[@]} -gt 0 ]]; then
  printf '❌ %d models are NOT compatible with %s:\n' "${#model_failed[@]}" "$dialect"
  printf '    - %s\n' "${model_failed[@]}"
fi
exit 1
