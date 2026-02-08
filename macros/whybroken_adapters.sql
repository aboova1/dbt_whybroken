{# ------------------------------------------------------------------ #}
{# Adapter-aware type mappings                                         #}
{# All return clean values via return() to avoid whitespace issues     #}
{# ------------------------------------------------------------------ #}

{% macro whybroken_type_string() %}
  {% if target.type == 'bigquery' %}{{ return('STRING') }}
  {% elif target.type in ('databricks', 'spark') %}{{ return('STRING') }}
  {% elif target.type in ('postgres', 'redshift') %}{{ return('VARCHAR(4096)') }}
  {% elif target.type == 'snowflake' %}{{ return('VARCHAR') }}
  {% else %}{{ return('VARCHAR') }}
  {% endif %}
{% endmacro %}

{% macro whybroken_type_int() %}
  {% if target.type == 'bigquery' %}{{ return('INT64') }}
  {% else %}{{ return('INT') }}
  {% endif %}
{% endmacro %}

{% macro whybroken_type_bigint() %}
  {% if target.type == 'bigquery' %}{{ return('INT64') }}
  {% else %}{{ return('BIGINT') }}
  {% endif %}
{% endmacro %}

{% macro whybroken_type_float() %}
  {% if target.type == 'bigquery' %}{{ return('FLOAT64') }}
  {% elif target.type == 'snowflake' %}{{ return('FLOAT') }}
  {% else %}{{ return('DOUBLE') }}
  {% endif %}
{% endmacro %}

{% macro whybroken_type_timestamp() %}
  {% if target.type == 'snowflake' %}{{ return('TIMESTAMP_NTZ') }}
  {% else %}{{ return('TIMESTAMP') }}
  {% endif %}
{% endmacro %}

{% macro whybroken_current_timestamp() %}
  {% if target.type in ('postgres', 'redshift') %}{{ return('NOW()') }}
  {% elif target.type == 'duckdb' %}{{ return('CURRENT_TIMESTAMP') }}
  {% else %}{{ return('CURRENT_TIMESTAMP()') }}
  {% endif %}
{% endmacro %}


{# ------------------------------------------------------------------ #}
{# Fully-qualified schema for WhyBroken tables                        #}
{# ------------------------------------------------------------------ #}

{% macro whybroken_fq_schema() %}
  {% set wb_schema = target.schema ~ '_whybroken' %}
  {% if target.type in ('databricks', 'spark') %}
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
  {% if target.type in ('bigquery', 'databricks', 'spark', 'snowflake') %}
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
    {{ return("SELECT column_name, data_type FROM " ~ database ~ "." ~ schema ~ ".INFORMATION_SCHEMA.COLUMNS WHERE table_name = '" ~ table_name ~ "' ORDER BY ordinal_position") }}
  {% elif target.type in ('databricks', 'spark') %}
    {{ return("SELECT column_name, data_type FROM " ~ database ~ ".information_schema.columns WHERE table_schema = '" ~ schema ~ "' AND table_name = '" ~ table_name ~ "' ORDER BY ordinal_position") }}
  {% elif target.type == 'snowflake' %}
    {{ return("SELECT column_name, data_type FROM " ~ database ~ ".information_schema.columns WHERE table_schema = UPPER('" ~ schema ~ "') AND table_name = UPPER('" ~ table_name ~ "') ORDER BY ordinal_position") }}
  {% else %}
    {{ return("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = '" ~ schema ~ "' AND table_name = '" ~ table_name ~ "' ORDER BY ordinal_position") }}
  {% endif %}
{% endmacro %}


{# ------------------------------------------------------------------ #}
{# Check if a column type is numeric                                   #}
{# ------------------------------------------------------------------ #}

{% macro whybroken_is_numeric(col_type) %}
  {% set ct = col_type | upper | trim %}
  {% if ct in ('INT', 'INTEGER', 'BIGINT', 'SMALLINT', 'TINYINT', 'FLOAT', 'DOUBLE', 'REAL', 'FLOAT64', 'INT64', 'BIGNUMERIC', 'LONG', 'SHORT') %}
    {{ return(true) }}
  {% elif ct.startswith('DECIMAL') or ct.startswith('NUMERIC') or ct.startswith('NUMBER') %}
    {{ return(true) }}
  {% else %}
    {{ return(false) }}
  {% endif %}
{% endmacro %}
