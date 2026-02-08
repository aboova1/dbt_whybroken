{% macro whybroken_create_tracking_tables() %}

  {% set wb_schema = whybroken.whybroken_fq_schema() %}
  {% do run_query("CREATE SCHEMA IF NOT EXISTS " ~ wb_schema) %}

  {% do run_query("CREATE TABLE IF NOT EXISTS " ~ wb_schema ~ ".whybroken_runs (run_id " ~ whybroken.whybroken_type_string() ~ ", started_at " ~ whybroken.whybroken_type_timestamp() ~ ", model_count " ~ whybroken.whybroken_type_int() ~ ", dbt_version " ~ whybroken.whybroken_type_string() ~ ")") %}

  {% do run_query("CREATE TABLE IF NOT EXISTS " ~ wb_schema ~ ".whybroken_snapshots (run_id " ~ whybroken.whybroken_type_string() ~ ", model_name " ~ whybroken.whybroken_type_string() ~ ", row_count " ~ whybroken.whybroken_type_bigint() ~ ", column_count " ~ whybroken.whybroken_type_int() ~ ", captured_at " ~ whybroken.whybroken_type_timestamp() ~ ")") %}

  {% do run_query("CREATE TABLE IF NOT EXISTS " ~ wb_schema ~ ".whybroken_column_stats (run_id " ~ whybroken.whybroken_type_string() ~ ", model_name " ~ whybroken.whybroken_type_string() ~ ", column_name " ~ whybroken.whybroken_type_string() ~ ", null_count " ~ whybroken.whybroken_type_bigint() ~ ", distinct_count " ~ whybroken.whybroken_type_bigint() ~ ", min_value " ~ whybroken.whybroken_type_string() ~ ", max_value " ~ whybroken.whybroken_type_string() ~ ", avg_value " ~ whybroken.whybroken_type_float() ~ ", captured_at " ~ whybroken.whybroken_type_timestamp() ~ ")") %}

  {% do run_query("CREATE TABLE IF NOT EXISTS " ~ wb_schema ~ ".whybroken_anomalies (run_id " ~ whybroken.whybroken_type_string() ~ ", model_name " ~ whybroken.whybroken_type_string() ~ ", anomaly_type " ~ whybroken.whybroken_type_string() ~ ", column_name " ~ whybroken.whybroken_type_string() ~ ", severity " ~ whybroken.whybroken_type_string() ~ ", description " ~ whybroken.whybroken_type_string() ~ ", current_value " ~ whybroken.whybroken_type_string() ~ ", previous_value " ~ whybroken.whybroken_type_string() ~ ", delta_pct " ~ whybroken.whybroken_type_float() ~ ", detected_at " ~ whybroken.whybroken_type_timestamp() ~ ")") %}

{% endmacro %}


{% macro whybroken_capture() %}

  {{ whybroken.whybroken_create_tracking_tables() }}

  {% set wb_schema = whybroken.whybroken_fq_schema() %}
  {% set run_id = invocation_id %}
  {% set ts = whybroken.whybroken_current_timestamp() %}
  {% set str_type = whybroken.whybroken_type_string() %}
  {% set float_type = whybroken.whybroken_type_float() %}

  {% set successful_models = results | selectattr('node.resource_type', 'equalto', 'model') | selectattr('status', 'equalto', 'success') | list %}

  {% if successful_models | length == 0 %}
    {{ return('') }}
  {% endif %}

  {% do run_query("INSERT INTO " ~ wb_schema ~ ".whybroken_runs VALUES ('" ~ run_id ~ "', " ~ ts ~ ", " ~ (successful_models | length) ~ ", '" ~ dbt_version ~ "')") %}

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
    {% set row_count = count_result.columns[0].values()[0] %}

    {% do run_query("INSERT INTO " ~ wb_schema ~ ".whybroken_snapshots VALUES ('" ~ run_id ~ "', '" ~ model_name ~ "', " ~ row_count ~ ", " ~ col_count ~ ", " ~ ts ~ ")") %}

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

      {% set stat_values = [] %}
      {% for srow in stats_result.rows %}
        {% do stat_values.append(
          "('" ~ run_id ~ "', '"
          ~ model_name ~ "', '"
          ~ srow[0] ~ "', "
          ~ srow[1] | string ~ ", "
          ~ srow[2] | string ~ ", '"
          ~ (srow[3] | replace("'", "''") if srow[3] is not none else "") ~ "', '"
          ~ (srow[4] | replace("'", "''") if srow[4] is not none else "") ~ "', "
          ~ (srow[5] | string if srow[5] is not none else "NULL") ~ ", "
          ~ ts ~ ")"
        ) %}
      {% endfor %}
      {% do run_query("INSERT INTO " ~ wb_schema ~ ".whybroken_column_stats VALUES " ~ (stat_values | join(", "))) %}
    {% endif %}
  {% endfor %}

  {{ whybroken.whybroken_detect_anomalies_batch(run_id) }}

  {% set anomaly_results = run_query(
    "SELECT severity, model_name, anomaly_type, column_name, current_value, previous_value, delta_pct "
    ~ "FROM " ~ wb_schema ~ ".whybroken_anomalies "
    ~ "WHERE run_id = '" ~ run_id ~ "' "
    ~ "ORDER BY CASE severity WHEN 'critical' THEN 1 WHEN 'high' THEN 2 WHEN 'medium' THEN 3 ELSE 4 END"
  ) %}

  {% set anomaly_rows = anomaly_results.rows | list %}
  {% set critical_count = anomaly_rows | selectattr(0, 'equalto', 'critical') | list | length %}
  {% set high_count = anomaly_rows | selectattr(0, 'equalto', 'high') | list | length %}
  {% set medium_count = anomaly_rows | selectattr(0, 'equalto', 'medium') | list | length %}
  {% set total_count = anomaly_rows | length %}

  {% if total_count == 0 %}
    {{ log("WhyBroken: 0 anomalies detected", info=True) }}
  {% else %}
    {% set parts = [] %}
    {% if critical_count > 0 %}{% do parts.append(critical_count ~ " critical") %}{% endif %}
    {% if high_count > 0 %}{% do parts.append(high_count ~ " high") %}{% endif %}
    {% if medium_count > 0 %}{% do parts.append(medium_count ~ " medium") %}{% endif %}
    {{ log("WhyBroken WARNING: " ~ total_count ~ " anomalies detected (" ~ parts | join(", ") ~ ")", info=True) }}

    {% for arow in anomaly_rows %}
      {% if arow[0] == 'critical' or arow[0] == 'high' %}
        {% set sev_label = arow[0] | upper %}
        {% set model = arow[1] %}
        {% set anom_type = arow[2] %}
        {% set col = arow[3] %}
        {% set curr = arow[4] %}
        {% set prev = arow[5] %}
        {% set delta = arow[6] %}
        {% if anom_type == 'row_count' %}
          {{ log("WhyBroken " ~ sev_label ~ ": " ~ model ~ " — Row count changed by " ~ (delta | round(1)) ~ "% (" ~ prev ~ " -> " ~ curr ~ ")", info=True) }}
        {% elif col is not none and col != '' %}
          {{ log("WhyBroken " ~ sev_label ~ ": " ~ model ~ " — " ~ anom_type | replace("_", " ") | capitalize ~ " of " ~ col ~ " changed by " ~ (delta | round(1)) ~ "% (" ~ prev ~ " -> " ~ curr ~ ")", info=True) }}
        {% else %}
          {{ log("WhyBroken " ~ sev_label ~ ": " ~ model ~ " — " ~ anom_type | replace("_", " ") | capitalize ~ " changed by " ~ (delta | round(1)) ~ "% (" ~ prev ~ " -> " ~ curr ~ ")", info=True) }}
        {% endif %}
      {% endif %}
    {% endfor %}
  {% endif %}

  {{ log("WhyBroken: captured " ~ (successful_models | length) ~ " models", info=True) }}

{% endmacro %}
