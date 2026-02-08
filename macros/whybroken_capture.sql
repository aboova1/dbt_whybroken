{% macro whybroken_create_tracking_tables() %}

  {% set wb_schema = whybroken.whybroken_fq_schema() %}

  {% if target.type in ('databricks', 'spark') %}
    {% do run_query("CREATE SCHEMA IF NOT EXISTS " ~ wb_schema) %}
  {% elif target.type == 'bigquery' %}
    {% do run_query("CREATE SCHEMA IF NOT EXISTS " ~ wb_schema) %}
  {% else %}
    {% do run_query("CREATE SCHEMA IF NOT EXISTS " ~ wb_schema) %}
  {% endif %}

  {% set runs_ddl %}
    CREATE TABLE IF NOT EXISTS {{ wb_schema }}.whybroken_runs (
      run_id            {{ whybroken.whybroken_type_string() }},
      started_at        {{ whybroken.whybroken_type_timestamp() }},
      completed_at      {{ whybroken.whybroken_type_timestamp() }},
      model_count       {{ whybroken.whybroken_type_int() }},
      invocation_id     {{ whybroken.whybroken_type_string() }},
      dbt_version       {{ whybroken.whybroken_type_string() }}
    )
  {% endset %}
  {% do run_query(runs_ddl) %}

  {% set snapshots_ddl %}
    CREATE TABLE IF NOT EXISTS {{ wb_schema }}.whybroken_snapshots (
      run_id            {{ whybroken.whybroken_type_string() }},
      model_name        {{ whybroken.whybroken_type_string() }},
      schema_name       {{ whybroken.whybroken_type_string() }},
      row_count         {{ whybroken.whybroken_type_bigint() }},
      column_count      {{ whybroken.whybroken_type_int() }},
      columns_csv       {{ whybroken.whybroken_type_string() }},
      captured_at       {{ whybroken.whybroken_type_timestamp() }}
    )
  {% endset %}
  {% do run_query(snapshots_ddl) %}

  {% set stats_ddl %}
    CREATE TABLE IF NOT EXISTS {{ wb_schema }}.whybroken_column_stats (
      run_id            {{ whybroken.whybroken_type_string() }},
      model_name        {{ whybroken.whybroken_type_string() }},
      column_name       {{ whybroken.whybroken_type_string() }},
      column_type       {{ whybroken.whybroken_type_string() }},
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
    on-run-end hook. Captures row counts, column stats, and detects anomalies
    for every successful model in the current dbt run.

    Usage in dbt_project.yml:
      on-run-end:
        - "{{ whybroken.whybroken_capture() }}"
  #}

  {{ whybroken.whybroken_create_tracking_tables() }}

  {% set wb_schema = whybroken.whybroken_fq_schema() %}
  {% set run_id = invocation_id %}

  {% set successful_models = results | selectattr('node.resource_type', 'equalto', 'model') | selectattr('status', 'equalto', 'success') | list %}

  {% set insert_run %}
    INSERT INTO {{ wb_schema }}.whybroken_runs
    VALUES (
      '{{ run_id }}',
      {{ whybroken.whybroken_current_timestamp() }},
      {{ whybroken.whybroken_current_timestamp() }},
      {{ successful_models | length }},
      '{{ invocation_id }}',
      '{{ dbt_version }}'
    )
  {% endset %}
  {% do run_query(insert_run) %}

  {% for result in successful_models %}
    {% set model_name = result.node.name %}
    {% set model_schema = result.node.schema %}
    {% set model_database = result.node.database %}
    {% set model_alias = result.node.alias or model_name %}
    {% set fqn = whybroken.whybroken_fq_table(model_database, model_schema, model_alias) %}

    {% set count_result = run_query("SELECT COUNT(*) AS cnt FROM " ~ fqn) %}
    {% set row_count = count_result.columns[0].values()[0] %}

    {% set col_result = run_query(whybroken.whybroken_get_columns_query(model_database, model_schema, model_alias)) %}
    {% set col_count = col_result.rows | length %}

    {% set col_names = [] %}
    {% for row in col_result.rows %}
      {% do col_names.append(row[0]) %}
    {% endfor %}

    {% set insert_snap %}
      INSERT INTO {{ wb_schema }}.whybroken_snapshots
      VALUES (
        '{{ run_id }}',
        '{{ model_name }}',
        '{{ model_schema }}',
        {{ row_count }},
        {{ col_count }},
        '{{ col_names | join(",") }}',
        {{ whybroken.whybroken_current_timestamp() }}
      )
    {% endset %}
    {% do run_query(insert_snap) %}

    {% for col_row in col_result.rows %}
      {% set col_name = col_row[0] %}
      {% set col_type = col_row[1] | upper %}

      {% if whybroken.whybroken_is_numeric(col_type) %}
        {% set stats_query %}
          SELECT
            COUNT(*) - COUNT({{ adapter.quote(col_name) }}) AS null_count,
            COUNT(DISTINCT {{ adapter.quote(col_name) }}) AS distinct_count,
            CAST(MIN({{ adapter.quote(col_name) }}) AS {{ whybroken.whybroken_type_string() }}) AS min_value,
            CAST(MAX({{ adapter.quote(col_name) }}) AS {{ whybroken.whybroken_type_string() }}) AS max_value,
            CAST(AVG(CAST({{ adapter.quote(col_name) }} AS {{ whybroken.whybroken_type_float() }})) AS {{ whybroken.whybroken_type_float() }}) AS avg_value
          FROM {{ fqn }}
        {% endset %}
      {% else %}
        {% set stats_query %}
          SELECT
            COUNT(*) - COUNT({{ adapter.quote(col_name) }}) AS null_count,
            COUNT(DISTINCT {{ adapter.quote(col_name) }}) AS distinct_count,
            CAST(MIN({{ adapter.quote(col_name) }}) AS {{ whybroken.whybroken_type_string() }}) AS min_value,
            CAST(MAX({{ adapter.quote(col_name) }}) AS {{ whybroken.whybroken_type_string() }}) AS max_value,
            CAST(NULL AS {{ whybroken.whybroken_type_float() }}) AS avg_value
          FROM {{ fqn }}
        {% endset %}
      {% endif %}

      {% set stats_result = run_query(stats_query) %}
      {% set srow = stats_result.rows[0] %}

      {% set insert_stats %}
        INSERT INTO {{ wb_schema }}.whybroken_column_stats
        VALUES (
          '{{ run_id }}',
          '{{ model_name }}',
          '{{ col_name }}',
          '{{ col_type }}',
          {{ srow[0] }},
          {{ srow[1] }},
          '{{ srow[2] | replace("'", "''") if srow[2] is not none else "" }}',
          '{{ srow[3] | replace("'", "''") if srow[3] is not none else "" }}',
          {{ srow[4] if srow[4] is not none else 'NULL' }},
          {{ whybroken.whybroken_current_timestamp() }}
        )
      {% endset %}
      {% do run_query(insert_stats) %}
    {% endfor %}

    {{ whybroken.whybroken_detect_anomalies(run_id, model_name) }}

  {% endfor %}

  {{ log("WhyBroken: captured " ~ (successful_models | length) ~ " models", info=True) }}

{% endmacro %}
