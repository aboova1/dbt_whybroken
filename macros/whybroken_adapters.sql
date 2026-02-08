{# ------------------------------------------------------------------ #}
{# Adapter-aware type mappings and helpers                             #}
{# ------------------------------------------------------------------ #}

{% macro whybroken_type_string() %}
  {% if target.type == 'bigquery' %}STRING
  {% elif target.type == 'databricks' %}STRING
  {% elif target.type in ('postgres', 'redshift') %}VARCHAR(4096)
  {% elif target.type == 'snowflake' %}VARCHAR
  {% elif target.type == 'duckdb' %}VARCHAR
  {% else %}VARCHAR(4096)
  {% endif %}
{% endmacro %}

{% macro whybroken_type_int() %}
  {% if target.type == 'bigquery' %}INT64
  {% else %}INT
  {% endif %}
{% endmacro %}

{% macro whybroken_type_bigint() %}
  {% if target.type == 'bigquery' %}INT64
  {% else %}BIGINT
  {% endif %}
{% endmacro %}

{% macro whybroken_type_float() %}
  {% if target.type == 'bigquery' %}FLOAT64
  {% elif target.type == 'snowflake' %}FLOAT
  {% else %}DOUBLE
  {% endif %}
{% endmacro %}

{% macro whybroken_type_timestamp() %}
  {% if target.type == 'bigquery' %}TIMESTAMP
  {% elif target.type == 'snowflake' %}TIMESTAMP_NTZ
  {% else %}TIMESTAMP
  {% endif %}
{% endmacro %}

{% macro whybroken_current_timestamp() %}
  {% if target.type == 'bigquery' %}CURRENT_TIMESTAMP()
  {% elif target.type == 'snowflake' %}CURRENT_TIMESTAMP()
  {% elif target.type == 'databricks' %}CURRENT_TIMESTAMP()
  {% elif target.type in ('postgres', 'redshift') %}NOW()
  {% elif target.type == 'duckdb' %}CURRENT_TIMESTAMP
  {% else %}CURRENT_TIMESTAMP
  {% endif %}
{% endmacro %}


{# ------------------------------------------------------------------ #}
{# Fully-qualified schema for WhyBroken tables                        #}
{# ------------------------------------------------------------------ #}

{% macro whybroken_fq_schema() %}
  {% set wb_schema = target.schema ~ '_whybroken' %}
  {% if target.type == 'databricks' %}
    {{ return(target.catalog ~ '.' ~ wb_schema) }}
  {% elif target.type == 'bigquery' %}
    {{ return(target.project ~ '.' ~ wb_schema) }}
  {% else %}
    {{ return(wb_schema) }}
  {% endif %}
{% endmacro %}


{# ------------------------------------------------------------------ #}
{# Fully-qualified table reference                                     #}
{# ------------------------------------------------------------------ #}

{% macro whybroken_fq_table(database, schema, table_name) %}
  {% if target.type == 'bigquery' %}
    {{ return(database ~ '.' ~ schema ~ '.' ~ table_name) }}
  {% elif target.type == 'databricks' %}
    {{ return(database ~ '.' ~ schema ~ '.' ~ table_name) }}
  {% elif target.type == 'snowflake' %}
    {{ return(database ~ '.' ~ schema ~ '.' ~ table_name) }}
  {% else %}
    {{ return(schema ~ '.' ~ table_name) }}
  {% endif %}
{% endmacro %}


{# ------------------------------------------------------------------ #}
{# Get column metadata query (adapter-aware)                           #}
{# ------------------------------------------------------------------ #}

{% macro whybroken_get_columns_query(database, schema, table_name) %}
  {% if target.type == 'bigquery' %}
    SELECT column_name, data_type
    FROM {{ database }}.{{ schema }}.INFORMATION_SCHEMA.COLUMNS
    WHERE table_name = '{{ table_name }}'
    ORDER BY ordinal_position
  {% elif target.type == 'databricks' %}
    SELECT column_name, data_type
    FROM {{ database }}.information_schema.columns
    WHERE table_schema = '{{ schema }}'
      AND table_name = '{{ table_name }}'
    ORDER BY ordinal_position
  {% elif target.type == 'snowflake' %}
    SELECT column_name, data_type
    FROM {{ database }}.information_schema.columns
    WHERE table_schema = UPPER('{{ schema }}')
      AND table_name = UPPER('{{ table_name }}')
    ORDER BY ordinal_position
  {% else %}
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = '{{ schema }}'
      AND table_name = '{{ table_name }}'
    ORDER BY ordinal_position
  {% endif %}
{% endmacro %}


{# ------------------------------------------------------------------ #}
{# Check if a column type is numeric                                   #}
{# ------------------------------------------------------------------ #}

{% macro whybroken_is_numeric(col_type) %}
  {% set numeric_types = [
    'INT', 'INTEGER', 'BIGINT', 'SMALLINT', 'TINYINT',
    'FLOAT', 'DOUBLE', 'DECIMAL', 'NUMERIC', 'REAL',
    'FLOAT64', 'INT64', 'NUMERIC', 'BIGNUMERIC',
    'NUMBER', 'LONG', 'SHORT',
  ] %}
  {% set ct = col_type | upper %}
  {% for nt in numeric_types %}
    {% if ct == nt or ct.startswith(nt ~ '(') or ct.startswith('DECIMAL') or ct.startswith('NUMBER') or ct.startswith('NUMERIC') %}
      {{ return(true) }}
    {% endif %}
  {% endfor %}
  {{ return(false) }}
{% endmacro %}
