{% macro whybroken_create_tracking_tables() %}

  {% set wb_schema = whybroken.whybroken_fq_schema() %}
  {% set s = whybroken.whybroken_type_string() %}
  {% set i = whybroken.whybroken_type_int() %}
  {% set bi = whybroken.whybroken_type_bigint() %}
  {% set f = whybroken.whybroken_type_float() %}
  {% set t = whybroken.whybroken_type_timestamp() %}

  {% do run_query("CREATE SCHEMA IF NOT EXISTS " ~ wb_schema) %}

  {% do run_query("CREATE TABLE IF NOT EXISTS " ~ wb_schema ~ ".whybroken_runs (run_id " ~ s ~ ", started_at " ~ t ~ ", model_count " ~ i ~ ", dbt_version " ~ s ~ ")") %}
  {% do run_query("CREATE TABLE IF NOT EXISTS " ~ wb_schema ~ ".whybroken_snapshots (run_id " ~ s ~ ", model_name " ~ s ~ ", row_count " ~ bi ~ ", column_count " ~ i ~ ", captured_at " ~ t ~ ")") %}
  {% do run_query("CREATE TABLE IF NOT EXISTS " ~ wb_schema ~ ".whybroken_column_stats (run_id " ~ s ~ ", model_name " ~ s ~ ", column_name " ~ s ~ ", null_count " ~ bi ~ ", distinct_count " ~ bi ~ ", min_value " ~ s ~ ", max_value " ~ s ~ ", avg_value " ~ f ~ ", captured_at " ~ t ~ ")") %}
  {% do run_query("CREATE TABLE IF NOT EXISTS " ~ wb_schema ~ ".whybroken_anomalies (run_id " ~ s ~ ", model_name " ~ s ~ ", anomaly_type " ~ s ~ ", column_name " ~ s ~ ", severity " ~ s ~ ", description " ~ s ~ ", current_value " ~ s ~ ", previous_value " ~ s ~ ", delta_pct " ~ f ~ ", detected_at " ~ t ~ ")") %}

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

  {% do run_query("INSERT INTO " ~ wb_schema ~ ".whybroken_runs (run_id, started_at, model_count, dbt_version) VALUES ('" ~ run_id ~ "', " ~ ts ~ ", " ~ (successful_models | length) ~ ", '" ~ dbt_version ~ "')") %}

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

    {% do run_query("INSERT INTO " ~ wb_schema ~ ".whybroken_snapshots (run_id, model_name, row_count, column_count, captured_at) VALUES ('" ~ run_id ~ "', '" ~ model_name ~ "', " ~ row_count ~ ", " ~ col_count ~ ", " ~ ts ~ ")") %}

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
      {% do run_query("INSERT INTO " ~ wb_schema ~ ".whybroken_column_stats (run_id, model_name, column_name, null_count, distinct_count, min_value, max_value, avg_value, captured_at) VALUES " ~ (stat_values | join(", "))) %}
    {% endif %}
  {% endfor %}

  {{ whybroken.whybroken_detect_anomalies_batch(run_id) }}

  {% set anomaly_results = run_query(
    "SELECT severity, model_name, anomaly_type, column_name, current_value, previous_value, delta_pct, description "
    ~ "FROM " ~ wb_schema ~ ".whybroken_anomalies "
    ~ "WHERE run_id = '" ~ run_id ~ "' "
    ~ "ORDER BY CASE severity WHEN 'critical' THEN 1 WHEN 'high' THEN 2 WHEN 'medium' THEN 3 ELSE 4 END"
  ) %}

  {% set anomaly_rows = anomaly_results.rows | list %}
  {% set critical_count = [] %}
  {% set high_count = [] %}
  {% set medium_count = [] %}
  {% for arow in anomaly_rows %}
    {% if arow[0] == 'critical' %}{% do critical_count.append(1) %}{% endif %}
    {% if arow[0] == 'high' %}{% do high_count.append(1) %}{% endif %}
    {% if arow[0] == 'medium' %}{% do medium_count.append(1) %}{% endif %}
  {% endfor %}
  {% set total_count = anomaly_rows | length %}

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

    {% for arow in anomaly_rows %}
      {% set sev = arow[0] | upper %}
      {% set model = arow[1] %}
      {% set anom_type = arow[2] %}
      {% set col = arow[3] %}
      {% set curr = arow[4] %}
      {% set prev = arow[5] %}
      {% set delta = arow[6] %}
      {% set desc = arow[7] %}

      {% if sev == 'CRITICAL' %}
        {% set marker = "[CRITICAL]" %}
      {% elif sev == 'HIGH' %}
        {% set marker = "[HIGH]    " %}
      {% else %}
        {% set marker = "[MEDIUM]  " %}
      {% endif %}

      {% if anom_type == 'row_count_change' %}
        {{ log(marker ~ "  " ~ model ~ "  >>  Row count shifted " ~ (delta | round(1)) ~ "% (was " ~ prev ~ ", now " ~ curr ~ ")", info=True) }}
      {% elif col is not none and col != '' and col != '*' %}
        {{ log(marker ~ "  " ~ model ~ "." ~ col ~ "  >>  " ~ desc, info=True) }}
      {% else %}
        {{ log(marker ~ "  " ~ model ~ "  >>  " ~ desc, info=True) }}
      {% endif %}
    {% endfor %}

    {{ log("", info=True) }}
    {{ log("Run `dbt run-operation whybroken.whybroken_show_anomalies` for full details.", info=True) }}
    {{ log("", info=True) }}
  {% endif %}

  {{ log("WhyBroken: captured " ~ (successful_models | length) ~ " models", info=True) }}

{% endmacro %}
