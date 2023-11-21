{%- set today = modules.datetime.date.today() | string -%}
{%- set n_days_ago = (modules.datetime.date.today() - modules.datetime.timedelta(days=2)) | string -%}
{%- set start_date = var("start_date", n_days_ago) -%}
{%- set end_date = var("end_date", today) -%}

{%- set partitions_to_replace = get_partitions_to_replace(start_date, end_date) -%}

{% if is_incremental() %}
    {{ log("Partitions to replace (start date/end date): " ~ partitions_to_replace.start_date ~ "/" ~ partitions_to_replace.end_date, info=true) }}
{% endif %}

-- Incremental strategy:
-- use incremental_strategy = "delete+insert" if merge key is not actually unique
-- but typically this reflects a data issue, so its better to determine why the key is not unique

-- Automatic clustering:
-- Don't enable this at first, as it will lead to additional costs
-- If model results are sorted, the data will be naturally clustered so that queries can prune
-- It's also important to understand how clustering works, so that you pick cluster keys with the appropriate legnth and cardinality given data volumes
-- cluster_by=['as_of_date','company_id', 'plan_id']

-- Transient:
-- Transient tables (dbt default behavior) have limited time travel and data retention, which can save storage costs
-- Often this is ok for dbt models, since they can be regenerated from raw data
-- Set transient=false to use normal storage, with time travel + failsafe

{{ config(
    materialized = "incremental",
    incremental_strategy = "merge",
    unique_key=['as_of_date','company_id','plan_id'],
    transient=true
    )
}}

WITH revenue_data AS (
    SELECT
        cr.as_of_date,
        cr.company_id,
        cr.plan_id,
        cr.revenue
    FROM {{ source('RAW_DEMO', 'company_revenue') }} cr
    {% if is_incremental() %}
    WHERE AS_OF_DATE BETWEEN '{{ partitions_to_replace.start_date }}' AND '{{ partitions_to_replace.end_date }}'
    {% endif %}
    ),
 plan_data AS (
     SELECT
         pd.as_of_date,
         pd.plan_id,
         pd.price
     FROM {{ source('RAW_DEMO', 'plan_details') }} pd
    {% if is_incremental() %}
    WHERE AS_OF_DATE BETWEEN '{{ partitions_to_replace.start_date }}' AND '{{ partitions_to_replace.end_date }}'
    {% endif %}
    )
SELECT
    rd.as_of_date,
    rd.company_id,
    rd.plan_id,
    rd.revenue,
    pd.price,
    rd.revenue / pd.price AS units_sold
FROM revenue_data rd
JOIN plan_data pd
    ON rd.plan_id = pd.plan_id
    AND rd.as_of_date = pd.as_of_date
-- always sort model data unless cluster keys are defined
-- dbt will automatically add a sort when tables have cluster keys
ORDER BY as_of_date, company_id, plan_id
