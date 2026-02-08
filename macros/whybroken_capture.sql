{% macro whybroken_create_baseline_table() %}

  {% set wb_schema = whybroken.whybroken_fq_schema() %}
  {% set s = whybroken.whybroken_type_string() %}
  {% set bi = whybroken.whybroken_type_bigint() %}
  {% set f = whybroken.whybroken_type_float() %}
  {% set t = whybroken.whybroken_type_timestamp() %}

  {% do run_query("CREATE SCHEMA IF NOT EXISTS " ~ wb_schema) %}

  {% do run_query(
    "CREATE TABLE IF NOT EXISTS " ~ wb_schema ~ ".whybroken_baseline ("
    ~ "model_name " ~ s ~ ", "
    ~ "row_count " ~ bi ~ ", "
    ~ "column_count " ~ bi ~ ", "
    ~ "column_name " ~ s ~ ", "
    ~ "null_count " ~ bi ~ ", "
    ~ "distinct_count " ~ bi ~ ", "
    ~ "min_value " ~ s ~ ", "
    ~ "max_value " ~ s ~ ", "
    ~ "avg_value " ~ f ~ ", "
    ~ "captured_at " ~ t
    ~ ")"
  ) %}

{% endmacro %}


{% macro whybroken_snapshot() %}

  {{ whybroken.whybroken_create_baseline_table() }}

  {% set wb_schema = whybroken.whybroken_fq_schema() %}
  {% set ts = whybroken.whybroken_current_timestamp() %}
  {% set str_type = whybroken.whybroken_type_string() %}
  {% set float_type = whybroken.whybroken_type_float() %}

  {% set model_nodes = [] %}
  {% for node in graph.nodes.values() %}
    {% if node.resource_type == 'model' %}
      {% do model_nodes.append(node) %}
    {% endif %}
  {% endfor %}

  {% if model_nodes | length == 0 %}
    {{ return('') }}
  {% endif %}

  {% set all_baseline_values = [] %}
  {% set snapshot_model_names = [] %}

  {% for node in model_nodes %}
    {% set model_name = node.name %}
    {% set model_alias = node.alias or model_name %}
    {% set model_schema = node.schema %}
    {% set model_database = node.database %}

    {% set relation = adapter.get_relation(database=model_database, schema=model_schema, identifier=model_alias) %}
    {% if relation is none %}
      {% do log("WhyBroken snapshot: " ~ model_name ~ " does not exist yet, skipping", info=True) %}
    {% else %}
      {% do snapshot_model_names.append(model_name) %}

      {% set fqn = whybroken.whybroken_fq_table(model_database, model_schema, model_alias) %}
      {% set col_query = whybroken.whybroken_get_columns_query(model_database, model_schema, model_alias) %}
      {% set col_data = run_query(col_query) %}
      {% set col_count = col_data.rows | length %}

      {% set stat_parts = [] %}
      {% set col_names = [] %}
      {% for row in col_data.rows %}
        {% set col_name = row[0] %}
        {% set col_type = row[1] %}
        {% set qcol = adapter.quote(col_name) %}
        {% set is_num = whybroken.whybroken_is_numeric(col_type | string | upper) %}
        {% do col_names.append({'name': col_name, 'is_numeric': is_num}) %}

        {% do stat_parts.append("COUNT(*) - COUNT(" ~ qcol ~ ")") %}
        {% do stat_parts.append("COUNT(DISTINCT " ~ qcol ~ ")") %}
        {% do stat_parts.append("CAST(MIN(" ~ qcol ~ ") AS " ~ str_type ~ ")") %}
        {% do stat_parts.append("CAST(MAX(" ~ qcol ~ ") AS " ~ str_type ~ ")") %}
        {% if is_num %}
          {% do stat_parts.append("CAST(AVG(CAST(" ~ qcol ~ " AS " ~ float_type ~ ")) AS " ~ float_type ~ ")") %}
        {% else %}
          {% do stat_parts.append("CAST(NULL AS " ~ float_type ~ ")") %}
        {% endif %}
      {% endfor %}

      {% if col_names | length == 0 %}
        {% set stats_query = "SELECT COUNT(*) FROM " ~ fqn %}
      {% else %}
        {% set stats_query = "SELECT COUNT(*), " ~ stat_parts | join(", ") ~ " FROM " ~ fqn %}
      {% endif %}
      {% set stats_result = run_query(stats_query) %}
      {% set srow = stats_result.rows[0] %}
      {% set row_count = srow[0] | float %}

      {% do all_baseline_values.append(
        "('" ~ model_name ~ "', " ~ row_count | int ~ ", " ~ col_count ~ ", '*', NULL, NULL, NULL, NULL, NULL, " ~ ts ~ ")"
      ) %}

      {% for i in range(col_names | length) %}
        {% set col_name = col_names[i]['name'] %}
        {% set offset = 1 + (i * 5) %}
        {% do all_baseline_values.append(
          "('" ~ model_name ~ "', " ~ row_count | int ~ ", " ~ col_count ~ ", '"
          ~ col_name ~ "', "
          ~ (srow[offset] | string if srow[offset] is not none else "NULL") ~ ", "
          ~ (srow[offset + 1] | string if srow[offset + 1] is not none else "NULL") ~ ", '"
          ~ (srow[offset + 2] | replace("'", "''") if srow[offset + 2] is not none else "") ~ "', '"
          ~ (srow[offset + 3] | replace("'", "''") if srow[offset + 3] is not none else "") ~ "', "
          ~ (srow[offset + 4] | string if srow[offset + 4] is not none else "NULL") ~ ", "
          ~ ts ~ ")"
        ) %}
      {% endfor %}
    {% endif %}
  {% endfor %}

  {% if snapshot_model_names | length > 0 %}
    {% set quoted_names = [] %}
    {% for n in snapshot_model_names %}
      {% do quoted_names.append("'" ~ n ~ "'") %}
    {% endfor %}
    {% do run_query("DELETE FROM " ~ wb_schema ~ ".whybroken_baseline WHERE model_name IN (" ~ quoted_names | join(', ') ~ ")") %}

    {% set batch_size = 50 %}
    {% for i in range(0, all_baseline_values | length, batch_size) %}
      {% set batch = all_baseline_values[i:i + batch_size] %}
      {% do run_query(
        "INSERT INTO " ~ wb_schema ~ ".whybroken_baseline "
        ~ "(model_name, row_count, column_count, column_name, null_count, distinct_count, min_value, max_value, avg_value, captured_at) "
        ~ "VALUES " ~ (batch | join(", "))
      ) %}
    {% endfor %}
  {% endif %}

  {{ log("WhyBroken: pre-run snapshot captured for " ~ snapshot_model_names | length ~ " models", info=True) }}

{% endmacro %}


{% macro whybroken_capture() %}

  {% set wb_schema = whybroken.whybroken_fq_schema() %}
  {% set str_type = whybroken.whybroken_type_string() %}
  {% set float_type = whybroken.whybroken_type_float() %}

  {% set row_count_threshold = var('whybroken_row_count_threshold', 50) %}
  {% set row_count_critical = var('whybroken_row_count_critical', 100) %}
  {% set avg_value_threshold = var('whybroken_avg_value_threshold', 30) %}
  {% set avg_value_critical = var('whybroken_avg_value_critical', 80) %}
  {% set avg_value_high = var('whybroken_avg_value_high', 50) %}
  {% set null_spike_threshold = var('whybroken_null_spike_threshold', 10) %}
  {% set fail_on_critical = var('whybroken_fail_on_critical', true) %}
  {% set auto_rollback = var('whybroken_auto_rollback', true) %}

  {% set successful_models = results | selectattr('node.resource_type', 'equalto', 'model') | selectattr('status', 'equalto', 'success') | list %}

  {% if successful_models | length == 0 %}
    {{ return('') }}
  {% endif %}

  {% set all_prev %}
    SELECT model_name, column_name, row_count, null_count, distinct_count, min_value, max_value, avg_value
    FROM {{ wb_schema }}.whybroken_baseline
  {% endset %}
  {% set all_prev = run_query(all_prev) %}

  {% set prev_data = {} %}
  {% for prow in all_prev.rows %}
    {% set mname = prow[0] %}
    {% if mname not in prev_data %}
      {% do prev_data.update({mname: {}}) %}
    {% endif %}
    {% do prev_data[mname].update({prow[1]: {
      'row_count': prow[2] | float if prow[2] is not none else none,
      'null_count': prow[3] | float if prow[3] is not none else none,
      'distinct_count': prow[4],
      'min_value': prow[5],
      'max_value': prow[6],
      'avg_value': prow[7] | float if prow[7] is not none else none
    }}) %}
  {% endfor %}

  {% set all_anomalies = [] %}

  {% for result in successful_models %}
    {% set model_name = result.node.name %}
    {% set model_alias = result.node.alias or model_name %}
    {% set model_schema = result.node.schema %}
    {% set model_database = result.node.database %}
    {% set fqn = whybroken.whybroken_fq_table(model_database, model_schema, model_alias) %}

    {% set col_query = whybroken.whybroken_get_columns_query(model_database, model_schema, model_alias) %}
    {% set col_data = run_query(col_query) %}

    {% set stat_parts = [] %}
    {% set col_names = [] %}
    {% for row in col_data.rows %}
      {% set col_name = row[0] %}
      {% set col_type = row[1] %}
      {% set qcol = adapter.quote(col_name) %}
      {% set is_num = whybroken.whybroken_is_numeric(col_type | string | upper) %}
      {% do col_names.append({'name': col_name, 'is_numeric': is_num}) %}

      {% do stat_parts.append("COUNT(*) - COUNT(" ~ qcol ~ ")") %}
      {% do stat_parts.append("COUNT(DISTINCT " ~ qcol ~ ")") %}
      {% do stat_parts.append("CAST(MIN(" ~ qcol ~ ") AS " ~ str_type ~ ")") %}
      {% do stat_parts.append("CAST(MAX(" ~ qcol ~ ") AS " ~ str_type ~ ")") %}
      {% if is_num %}
        {% do stat_parts.append("CAST(AVG(CAST(" ~ qcol ~ " AS " ~ float_type ~ ")) AS " ~ float_type ~ ")") %}
      {% else %}
        {% do stat_parts.append("CAST(NULL AS " ~ float_type ~ ")") %}
      {% endif %}
    {% endfor %}

    {% if col_names | length == 0 %}
      {% set stats_query = "SELECT COUNT(*) FROM " ~ fqn %}
    {% else %}
      {% set stats_query = "SELECT COUNT(*), " ~ stat_parts | join(", ") ~ " FROM " ~ fqn %}
    {% endif %}
    {% set stats_result = run_query(stats_query) %}
    {% set srow = stats_result.rows[0] %}
    {% set row_count = srow[0] | float %}

    {% set prev_map = prev_data[model_name] if model_name in prev_data else {} %}

    {% if '*' in prev_map %}
      {% set prev_row_count = prev_map['*']['row_count'] %}
      {% if prev_row_count is not none and prev_row_count > 0 %}
        {% set row_delta = ((row_count - prev_row_count) * 100.0 / prev_row_count) %}
        {% if row_delta | abs > row_count_threshold %}
          {% set row_sev = 'critical' if row_delta | abs > row_count_critical else 'high' %}
          {% do all_anomalies.append({
            'severity': row_sev,
            'model': model_name,
            'type': 'row_count_change',
            'column': '*',
            'fqn': fqn,
            'materialization': result.node.config.materialized,
            'description': 'Row count changed by ' ~ (row_delta | round(1)) ~ '% (' ~ prev_row_count | int ~ ' -> ' ~ row_count | int ~ ')'
          }) %}
        {% endif %}
      {% endif %}
    {% endif %}

    {% for i in range(col_names | length) %}
      {% set col_name = col_names[i]['name'] %}
      {% set offset = 1 + (i * 5) %}
      {% set cur_null = srow[offset] | float if srow[offset] is not none else none %}
      {% set cur_distinct = srow[offset + 1] %}
      {% set cur_min = srow[offset + 2] %}
      {% set cur_max = srow[offset + 3] %}
      {% set cur_avg = srow[offset + 4] | float if srow[offset + 4] is not none else none %}

      {% if col_name in prev_map %}
        {% set prev = prev_map[col_name] %}
        {% set prev_null = prev['null_count'] %}
        {% set prev_avg = prev['avg_value'] %}

        {% if cur_avg is not none and prev_avg is not none and prev_avg != 0 %}
          {% set avg_delta = ((cur_avg - prev_avg) * 100.0 / (prev_avg | abs)) %}
          {% if avg_delta | abs > avg_value_threshold %}
            {% if avg_delta | abs > avg_value_critical %}
              {% set avg_sev = 'critical' %}
            {% elif avg_delta | abs > avg_value_high %}
              {% set avg_sev = 'high' %}
            {% else %}
              {% set avg_sev = 'medium' %}
            {% endif %}
            {% do all_anomalies.append({
              'severity': avg_sev,
              'model': model_name,
              'type': 'avg_value_change',
              'column': col_name,
              'fqn': fqn,
              'materialization': result.node.config.materialized,
              'description': 'Average of ' ~ col_name ~ ' changed by ' ~ (avg_delta | round(1)) ~ '% (' ~ (prev_avg | round(2)) ~ ' -> ' ~ (cur_avg | round(2)) ~ ')'
            }) %}
          {% endif %}
        {% endif %}

        {% if prev_null is not none and cur_null is not none and cur_null > prev_null and (cur_null - prev_null) > null_spike_threshold %}
          {% do all_anomalies.append({
            'severity': 'high',
            'model': model_name,
            'type': 'null_spike',
            'column': col_name,
            'fqn': fqn,
            'materialization': result.node.config.materialized,
            'description': 'Null count for ' ~ col_name ~ ' increased from ' ~ prev_null | int ~ ' to ' ~ cur_null | int
          }) %}
        {% endif %}
      {% endif %}
    {% endfor %}
  {% endfor %}

  {% set critical_count = [] %}
  {% set high_count = [] %}
  {% set medium_count = [] %}
  {% for a in all_anomalies %}
    {% if a.severity == 'critical' %}{% do critical_count.append(1) %}{% endif %}
    {% if a.severity == 'high' %}{% do high_count.append(1) %}{% endif %}
    {% if a.severity == 'medium' %}{% do medium_count.append(1) %}{% endif %}
  {% endfor %}
  {% set total_count = all_anomalies | length %}

  {% set sorted_anomalies = [] %}
  {% for a in all_anomalies %}{% if a.severity == 'critical' %}{% do sorted_anomalies.append(a) %}{% endif %}{% endfor %}
  {% for a in all_anomalies %}{% if a.severity == 'high' %}{% do sorted_anomalies.append(a) %}{% endif %}{% endfor %}
  {% for a in all_anomalies %}{% if a.severity == 'medium' %}{% do sorted_anomalies.append(a) %}{% endif %}{% endfor %}

  {% if total_count == 0 %}
    {{ log("", info=True) }}
    {{ log("=== WhyBroken: All clear. 0 anomalies detected across " ~ (successful_models | length) ~ " models. ===", info=True) }}
    {{ log("", info=True) }}
  {% endif %}

  {% if fail_on_critical and critical_count | length > 0 %}

    {% set rollback_models = {} %}
    {% for a in all_anomalies %}
      {% if a.materialization == 'table' and a.model not in rollback_models %}
        {% do rollback_models.update({a.model: a.fqn}) %}
      {% endif %}
    {% endfor %}

    {% set rolled_back = [] %}
    {% set rollback_failed = [] %}
    {% set skipped_views = [] %}

    {% if auto_rollback and rollback_models | length > 0 %}
      {% set baseline_ts_result = run_query(
        "SELECT captured_at FROM " ~ wb_schema ~ ".whybroken_baseline LIMIT 1"
      ) %}

      {% if baseline_ts_result.rows | length > 0 %}
        {% set restore_ts = baseline_ts_result.rows[0][0] %}

        {% for model_name, fqn in rollback_models.items() %}
          {% if target.type in ('databricks', 'spark') %}
            {% set restore_sql = "RESTORE TABLE " ~ fqn ~ " TO TIMESTAMP AS OF '" ~ restore_ts ~ "'" %}
            {% do run_query(restore_sql) %}
            {% do rolled_back.append(model_name) %}
          {% elif target.type == 'snowflake' %}
            {% set restore_sql = "CREATE OR REPLACE TABLE " ~ fqn ~ " CLONE " ~ fqn ~ " AT(TIMESTAMP => '" ~ restore_ts ~ "'::TIMESTAMP)" %}
            {% do run_query(restore_sql) %}
            {% do rolled_back.append(model_name) %}
          {% else %}
            {% do rollback_failed.append(model_name) %}
          {% endif %}
        {% endfor %}
      {% endif %}
    {% endif %}

    {% for a in all_anomalies %}
      {% if a.materialization != 'table' and a.model not in skipped_views %}
        {% do skipped_views.append(a.model) %}
      {% endif %}
    {% endfor %}

    {% set error_lines = [] %}
    {% do error_lines.append("") %}
    {% do error_lines.append("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!") %}
    {% do error_lines.append("!!!  WhyBroken ALERT: " ~ total_count ~ " data anomalies detected") %}
    {% set parts = [] %}
    {% if critical_count | length > 0 %}{% do parts.append(critical_count | length ~ " CRITICAL") %}{% endif %}
    {% if high_count | length > 0 %}{% do parts.append(high_count | length ~ " HIGH") %}{% endif %}
    {% if medium_count | length > 0 %}{% do parts.append(medium_count | length ~ " MEDIUM") %}{% endif %}
    {% do error_lines.append("!!!  Breakdown: " ~ parts | join(" / ")) %}
    {% do error_lines.append("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!") %}
    {% do error_lines.append("") %}

    {% for a in sorted_anomalies %}
      {% if a.severity == 'critical' %}
        {% set m = "[CRITICAL]" %}
      {% elif a.severity == 'high' %}
        {% set m = "[HIGH]    " %}
      {% else %}
        {% set m = "[MEDIUM]  " %}
      {% endif %}
      {% if a.type == 'row_count_change' %}
        {% do error_lines.append(m ~ "  " ~ a.model ~ "  >>  " ~ a.description) %}
      {% elif a.column is not none and a.column != '' and a.column != '*' %}
        {% do error_lines.append(m ~ "  " ~ a.model ~ "." ~ a.column ~ "  >>  " ~ a.description) %}
      {% else %}
        {% do error_lines.append(m ~ "  " ~ a.model ~ "  >>  " ~ a.description) %}
      {% endif %}
    {% endfor %}

    {% do error_lines.append("") %}

    {% if rolled_back | length > 0 %}
      {% do error_lines.append("ROLLED BACK " ~ rolled_back | length ~ " table(s) to pre-run state:") %}
      {% for rb in rolled_back %}
        {% do error_lines.append("  - " ~ rb ~ " (restored)") %}
      {% endfor %}
      {% do error_lines.append("") %}
    {% endif %}

    {% if skipped_views | length > 0 %}
      {% do error_lines.append("WARNING: " ~ skipped_views | length ~ " view(s) cannot be rolled back (views reflect source data):") %}
      {% for sv in skipped_views %}
        {% do error_lines.append("  - " ~ sv) %}
      {% endfor %}
      {% do error_lines.append("") %}
    {% endif %}

    {% if rollback_failed | length > 0 %}
      {% do error_lines.append("WARNING: " ~ rollback_failed | length ~ " table(s) could not be rolled back (adapter does not support time travel):") %}
      {% for rf in rollback_failed %}
        {% do error_lines.append("  - " ~ rf) %}
      {% endfor %}
      {% do error_lines.append("") %}
    {% endif %}

    {% if not auto_rollback %}
      {% do error_lines.append("Auto-rollback is DISABLED. Bad data is live in production.") %}
      {% do error_lines.append("To enable auto-rollback, set in dbt_project.yml:") %}
      {% do error_lines.append("  vars:") %}
      {% do error_lines.append("    whybroken_auto_rollback: true") %}
      {% do error_lines.append("") %}
    {% endif %}

    {% do error_lines.append("To disable auto-fail, add to dbt_project.yml:") %}
    {% do error_lines.append("  vars:") %}
    {% do error_lines.append("    whybroken_fail_on_critical: false") %}

    {{ exceptions.raise_compiler_error(error_lines | join("\n")) }}
  {% endif %}

{% endmacro %}
