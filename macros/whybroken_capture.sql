{% macro whybroken_create_tracking_tables() %}

  {% set wb_schema = whybroken.whybroken_fq_schema() %}
  {% do run_query("CREATE SCHEMA IF NOT EXISTS " ~ wb_schema) %}

  {% set runs_ddl %}
    CREATE TABLE IF NOT EXISTS {{ wb_schema }}.whybroken_runs (
      run_id            {{ whybroken.whybroken_type_string() }},
      started_at        {{ whybroken.whybroken_type_timestamp() }},
      model_count       {{ whybroken.whybroken_type_int() }},
      dbt_version       {{ whybroken.whybroken_type_string() }}
    )
  {% endset %}
  {% do run_query(runs_ddl) %}

  {% set snapshots_ddl %}
    CREATE TABLE IF NOT EXISTS {{ wb_schema }}.whybroken_snapshots (
      run_id            {{ whybroken.whybroken_type_string() }},
      model_name        {{ whybroken.whybroken_type_string() }},
      row_count         {{ whybroken.whybroken_type_bigint() }},
      column_count      {{ whybroken.whybroken_type_int() }},
      captured_at       {{ whybroken.whybroken_type_timestamp() }}
    )
  {% endset %}
  {% do run_query(snapshots_ddl) %}

  {% set stats_ddl %}
    CREATE TABLE IF NOT EXISTS {{ wb_schema }}.whybroken_column_stats (
      run_id            {{ whybroken.whybroken_type_string() }},
      model_name        {{ whybroken.whybroken_type_string() }},
      column_name       {{ whybroken.whybroken_type_string() }},
      null_count        {{ whybroken.whybroken_type_bigint() }},
      distinct_count    {{ whybroken.whybroken_type_bigint() }},
      min_value         {{ whybroken.whybroken_type_string() }},
      max_value         {{ whybroken.whybroken_type_string() }},
      avg_value         {{ whybroken.whybroken_type_float() }},
      captured_at       {{ whybroken.whybroken_type_timestamp() }}
    )
  {% endset %}
  {% do run_query(stats_ddl) %}

  {% set anomalies_ddl %}
    CREATE TABLE IF NOT EXISTS {{ wb_schema }}.whybroken_anomalies (
      run_id            {{ whybroken.whybroken_type_string() }},
      model_name        {{ whybroken.whybroken_type_string() }},
      anomaly_type      {{ whybroken.whybroken_type_string() }},
      column_name       {{ whybroken.whybroken_type_string() }},
      severity          {{ whybroken.whybroken_type_string() }},
      description       {{ whybroken.whybroken_type_string() }},
      current_value     {{ whybroken.whybroken_type_string() }},
      previous_value    {{ whybroken.whybroken_type_string() }},
      delta_pct         {{ whybroken.whybroken_type_float() }},
      detected_at       {{ whybroken.whybroken_type_timestamp() }}
    )
  {% endset %}
  {% do run_query(anomalies_ddl) %}

{% endmacro %}


{% macro whybroken_capture() %}
  {#
    on-run-end hook. Captures row counts and column stats for every
    successful model, then detects anomalies vs the previous run.

    Optimized: 3 queries per model (count + columns + batched stats)
    instead of 1 query per column.

    Usage in dbt_project.yml:
      on-run-end:
        - "{{ whybroken.whybroken_capture() }}"
  #}

  {{ whybroken.whybroken_create_tracking_tables() }}

  {% set wb_schema = whybroken.whybroken_fq_schema() %}
  {% set run_id = invocation_id %}
  {% set ts = whybroken.whybroken_current_timestamp() %}

  {% set successful_models = results | selectattr('node.resource_type', 'equalto', 'model') | selectattr('status', 'equalto', 'success') | list %}

  {# Record the run — 1 query #}
  {% set insert_run %}
    INSERT INTO {{ wb_schema }}.whybroken_runs
    VALUES ('{{ run_id }}', {{ ts }}, {{ successful_models | length }}, '{{ dbt_version }}')
  {% endset %}
  {% do run_query(insert_run) %}

  {# Process each model — 3 queries per model #}
  {% for result in successful_models %}
    {% set model_name = result.node.name %}
    {% set model_schema = result.node.schema %}
    {% set model_database = result.node.database %}
    {% set model_alias = result.node.alias or model_name %}
    {% set fqn = whybroken.whybroken_fq_table(model_database, model_schema, model_alias) %}

    {# Query 1: get columns #}
    {% set col_result = run_query(whybroken.whybroken_get_columns_query(model_database, model_schema, model_alias)) %}
    {% set columns = col_result.rows %}

    {# Query 2: single query gets row count + all column stats at once #}
    {% set stat_selects = [] %}
    {% for col_row in columns %}
      {% set col_name = col_row[0] %}
      {% set col_type = col_row[1] | upper %}
      {% set qcol = adapter.quote(col_name) %}

      {% if whybroken.whybroken_is_numeric(col_type) %}
        {% do stat_selects.append(
          "SELECT '" ~ col_name ~ "' AS col_name, "
          ~ "COUNT(*) - COUNT(" ~ qcol ~ ") AS null_count, "
          ~ "COUNT(DISTINCT " ~ qcol ~ ") AS distinct_count, "
          ~ "CAST(MIN(" ~ qcol ~ ") AS " ~ whybroken.whybroken_type_string() ~ ") AS min_val, "
          ~ "CAST(MAX(" ~ qcol ~ ") AS " ~ whybroken.whybroken_type_string() ~ ") AS max_val, "
          ~ "CAST(AVG(CAST(" ~ qcol ~ " AS " ~ whybroken.whybroken_type_float() ~ ")) AS " ~ whybroken.whybroken_type_float() ~ ") AS avg_val "
          ~ "FROM " ~ fqn
        ) %}
      {% else %}
        {% do stat_selects.append(
          "SELECT '" ~ col_name ~ "' AS col_name, "
          ~ "COUNT(*) - COUNT(" ~ qcol ~ ") AS null_count, "
          ~ "COUNT(DISTINCT " ~ qcol ~ ") AS distinct_count, "
          ~ "CAST(MIN(" ~ qcol ~ ") AS " ~ whybroken.whybroken_type_string() ~ ") AS min_val, "
          ~ "CAST(MAX(" ~ qcol ~ ") AS " ~ whybroken.whybroken_type_string() ~ ") AS max_val, "
          ~ "CAST(NULL AS " ~ whybroken.whybroken_type_float() ~ ") AS avg_val "
          ~ "FROM " ~ fqn
        ) %}
      {% endif %}
    {% endfor %}

    {% if stat_selects | length > 0 %}
      {% set batch_query = stat_selects | join(" UNION ALL ") %}
      {% set stats_result = run_query(batch_query) %}

      {# Query 3: batch insert snapshot + all column stats #}
      {% set row_count = 0 %}
      {% if stats_result.rows | length > 0 %}
        {# Row count = null_count + distinct_count isn't right; get it from first column's total #}
        {% set count_result = run_query("SELECT COUNT(*) FROM " ~ fqn) %}
        {% set row_count = count_result.columns[0].values()[0] %}
      {% endif %}

      {# Insert snapshot #}
      {% set insert_snap %}
        INSERT INTO {{ wb_schema }}.whybroken_snapshots
        VALUES ('{{ run_id }}', '{{ model_name }}', {{ row_count }}, {{ columns | length }}, {{ ts }})
      {% endset %}
      {% do run_query(insert_snap) %}

      {# Batch insert all column stats with UNION ALL #}
      {% set stat_values = [] %}
      {% for srow in stats_result.rows %}
        {% do stat_values.append(
          "SELECT "
          ~ "'" ~ run_id ~ "', "
          ~ "'" ~ model_name ~ "', "
          ~ "'" ~ srow[0] ~ "', "
          ~ srow[1] | string ~ ", "
          ~ srow[2] | string ~ ", "
          ~ "'" ~ (srow[3] | replace("'", "''") if srow[3] is not none else "") ~ "', "
          ~ "'" ~ (srow[4] | replace("'", "''") if srow[4] is not none else "") ~ "', "
          ~ (srow[5] | string if srow[5] is not none else "NULL") ~ ", "
          ~ ts
        ) %}
      {% endfor %}

      {% if stat_values | length > 0 %}
        {% set insert_all_stats %}
          INSERT INTO {{ wb_schema }}.whybroken_column_stats
          {{ stat_values | join(" UNION ALL ") }}
        {% endset %}
        {% do run_query(insert_all_stats) %}
      {% endif %}
    {% endif %}

    {{ whybroken.whybroken_detect_anomalies(run_id, model_name) }}

  {% endfor %}

  {{ log("WhyBroken: captured " ~ (successful_models | length) ~ " models", info=True) }}

{% endmacro %}
