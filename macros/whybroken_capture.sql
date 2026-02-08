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


{% macro whybroken_capture() %}

  {{ whybroken.whybroken_create_baseline_table() }}

  {% set wb_schema = whybroken.whybroken_fq_schema() %}
  {% set ts = whybroken.whybroken_current_timestamp() %}
  {% set str_type = whybroken.whybroken_type_string() %}
  {% set float_type = whybroken.whybroken_type_float() %}

  {% set row_count_threshold = var('whybroken_row_count_threshold', 50) %}
  {% set row_count_critical = var('whybroken_row_count_critical', 100) %}
  {% set avg_value_threshold = var('whybroken_avg_value_threshold', 30) %}
  {% set avg_value_critical = var('whybroken_avg_value_critical', 80) %}
  {% set avg_value_high = var('whybroken_avg_value_high', 50) %}
  {% set null_spike_threshold = var('whybroken_null_spike_threshold', 10) %}
  {% set fail_on_critical = var('whybroken_fail_on_critical', true) %}

  {% set successful_models = results | selectattr('node.resource_type', 'equalto', 'model') | selectattr('status', 'equalto', 'success') | list %}

  {% if successful_models | length == 0 %}
    {{ return('') }}
  {% endif %}

  {% set all_anomalies = [] %}

  {% for result in successful_models %}
    {% set model_name = result.node.name %}
    {% set model_alias = result.node.alias or model_name %}
    {% set model_schema = result.node.schema %}
    {% set model_database = result.node.database %}
    {% set fqn = whybroken.whybroken_fq_table(model_database, model_schema, model_alias) %}

    {% set col_query = whybroken.whybroken_get_columns_query(model_database, model_schema, model_alias) %}
    {% set col_data = run_query(col_query) %}
    {% set col_count = col_data.rows | length %}

    {% set count_result = run_query("SELECT COUNT(*) FROM " ~ fqn) %}
    {% set row_count = count_result.columns[0].values()[0] | float %}

    {% set prev_baseline = run_query(
      "SELECT column_name, row_count, null_count, distinct_count, min_value, max_value, avg_value "
      ~ "FROM " ~ wb_schema ~ ".whybroken_baseline "
      ~ "WHERE model_name = '" ~ model_name ~ "'"
    ) %}

    {% set prev_map = {} %}
    {% for prow in prev_baseline.rows %}
      {% do prev_map.update({prow[0]: {'row_count': prow[1] | float, 'null_count': prow[2] | float if prow[2] is not none else none, 'distinct_count': prow[3], 'min_value': prow[4], 'max_value': prow[5], 'avg_value': prow[6] | float if prow[6] is not none else none}}) %}
    {% endfor %}

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
            'description': 'Row count changed by ' ~ (row_delta | round(1)) ~ '% (' ~ prev_row_count | int ~ ' -> ' ~ row_count | int ~ ')',
            'current': row_count | int | string,
            'previous': prev_row_count | int | string,
            'delta': row_delta
          }) %}
        {% endif %}
      {% endif %}
    {% endif %}

    {% set stat_selects = [] %}
    {% for row in col_data.rows %}
      {% set col_name = row[0] %}
      {% set col_type = row[1] %}
      {% set qcol = adapter.quote(col_name) %}
      {% set is_num = whybroken.whybroken_is_numeric(col_type | string | upper) %}

      {% if is_num %}
        {% do stat_selects.append(
          "SELECT '" ~ col_name ~ "', "
          ~ "COUNT(*) - COUNT(" ~ qcol ~ "), "
          ~ "COUNT(DISTINCT " ~ qcol ~ "), "
          ~ "CAST(MIN(" ~ qcol ~ ") AS " ~ str_type ~ "), "
          ~ "CAST(MAX(" ~ qcol ~ ") AS " ~ str_type ~ "), "
          ~ "CAST(AVG(CAST(" ~ qcol ~ " AS " ~ float_type ~ ")) AS " ~ float_type ~ ") "
          ~ "FROM " ~ fqn
        ) %}
      {% else %}
        {% do stat_selects.append(
          "SELECT '" ~ col_name ~ "', "
          ~ "COUNT(*) - COUNT(" ~ qcol ~ "), "
          ~ "COUNT(DISTINCT " ~ qcol ~ "), "
          ~ "CAST(MIN(" ~ qcol ~ ") AS " ~ str_type ~ "), "
          ~ "CAST(MAX(" ~ qcol ~ ") AS " ~ str_type ~ "), "
          ~ "CAST(NULL AS " ~ float_type ~ ") "
          ~ "FROM " ~ fqn
        ) %}
      {% endif %}
    {% endfor %}

    {% if stat_selects | length > 0 %}
      {% set stats_result = run_query(stat_selects | join(" UNION ALL ")) %}

      {% for srow in stats_result.rows %}
        {% set col_name = srow[0] %}
        {% set cur_null = srow[1] | float if srow[1] is not none else none %}
        {% set cur_avg = srow[5] | float if srow[5] is not none else none %}

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
                'description': 'Average of ' ~ col_name ~ ' changed by ' ~ (avg_delta | round(1)) ~ '% (' ~ (prev_avg | round(2)) ~ ' -> ' ~ (cur_avg | round(2)) ~ ')',
                'current': cur_avg | string,
                'previous': prev_avg | string,
                'delta': avg_delta
              }) %}
            {% endif %}
          {% endif %}

          {% if prev_null is not none and cur_null is not none and cur_null > prev_null and (cur_null - prev_null) > null_spike_threshold %}
            {% do all_anomalies.append({
              'severity': 'high',
              'model': model_name,
              'type': 'null_spike',
              'column': col_name,
              'description': 'Null count for ' ~ col_name ~ ' increased from ' ~ prev_null ~ ' to ' ~ cur_null,
              'current': cur_null | string,
              'previous': prev_null | string,
              'delta': ((cur_null - prev_null) * 100.0 / prev_null) if prev_null > 0 else 100.0
            }) %}
          {% endif %}
        {% endif %}
      {% endfor %}
    {% endif %}

    {% do run_query(
      "DELETE FROM " ~ wb_schema ~ ".whybroken_baseline WHERE model_name = '" ~ model_name ~ "'"
    ) %}

    {% set baseline_values = [] %}
    {% do baseline_values.append(
      "('" ~ model_name ~ "', " ~ row_count ~ ", " ~ col_count ~ ", '*', NULL, NULL, NULL, NULL, NULL, " ~ ts ~ ")"
    ) %}

    {% if stat_selects | length > 0 %}
      {% for srow in stats_result.rows %}
        {% do baseline_values.append(
          "('" ~ model_name ~ "', " ~ row_count ~ ", " ~ col_count ~ ", '"
          ~ srow[0] ~ "', "
          ~ srow[1] | string ~ ", "
          ~ srow[2] | string ~ ", '"
          ~ (srow[3] | replace("'", "''") if srow[3] is not none else "") ~ "', '"
          ~ (srow[4] | replace("'", "''") if srow[4] is not none else "") ~ "', "
          ~ (srow[5] | string if srow[5] is not none else "NULL") ~ ", "
          ~ ts ~ ")"
        ) %}
      {% endfor %}
    {% endif %}

    {% do run_query(
      "INSERT INTO " ~ wb_schema ~ ".whybroken_baseline "
      ~ "(model_name, row_count, column_count, column_name, null_count, distinct_count, min_value, max_value, avg_value, captured_at) "
      ~ "VALUES " ~ (baseline_values | join(", "))
    ) %}
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
  {% else %}
    {{ log("", info=True) }}
    {{ log("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!", info=True) }}
    {{ log("!!!  WhyBroken ALERT: " ~ total_count ~ " data anomalies detected", info=True) }}
    {% set parts = [] %}
    {% if critical_count | length > 0 %}{% do parts.append(critical_count | length ~ " CRITICAL") %}{% endif %}
    {% if high_count | length > 0 %}{% do parts.append(high_count | length ~ " HIGH") %}{% endif %}
    {% if medium_count | length > 0 %}{% do parts.append(medium_count | length ~ " MEDIUM") %}{% endif %}
    {{ log("!!!  Breakdown: " ~ parts | join(" / "), info=True) }}
    {{ log("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!", info=True) }}
    {{ log("", info=True) }}

    {% for a in sorted_anomalies %}
      {% if a.severity == 'critical' %}
        {% set marker = "[CRITICAL]" %}
      {% elif a.severity == 'high' %}
        {% set marker = "[HIGH]    " %}
      {% else %}
        {% set marker = "[MEDIUM]  " %}
      {% endif %}

      {% if a.type == 'row_count_change' %}
        {{ log(marker ~ "  " ~ a.model ~ "  >>  " ~ a.description, info=True) }}
      {% elif a.column is not none and a.column != '' and a.column != '*' %}
        {{ log(marker ~ "  " ~ a.model ~ "." ~ a.column ~ "  >>  " ~ a.description, info=True) }}
      {% else %}
        {{ log(marker ~ "  " ~ a.model ~ "  >>  " ~ a.description, info=True) }}
      {% endif %}
    {% endfor %}

    {{ log("", info=True) }}
    {% if fail_on_critical and critical_count | length > 0 %}
      {{ log(">>> This run will FAIL because critical anomalies were found.", info=True) }}
      {{ log(">>> To disable auto-fail, set in dbt_project.yml:", info=True) }}
      {{ log(">>>   vars:", info=True) }}
      {{ log(">>>     whybroken_fail_on_critical: false", info=True) }}
      {{ log("", info=True) }}
    {% endif %}
  {% endif %}

  {{ log("WhyBroken: captured " ~ (successful_models | length) ~ " models", info=True) }}

  {% if fail_on_critical and critical_count | length > 0 %}
    {{ exceptions.raise_compiler_error("WhyBroken: CRITICAL anomalies detected. See alert details above. To disable auto-fail, add to dbt_project.yml: vars: {whybroken_fail_on_critical: false}") }}
  {% endif %}

{% endmacro %}
