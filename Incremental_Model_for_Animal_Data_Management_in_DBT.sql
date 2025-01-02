{{ 
config(
    materialized = 'incremental',
    partition_by={
        "field": "observation_date",
        "data_type": "date",
        "granularity": "day"},
    incremental_strategy = 'insert_overwrite',
    on_schema_change='sync_all_columns'
)
}}

-- This model creates an incremental table with partitioning and handles schema changes dynamically.
-- Adjusted column names and values for demonstration purposes.

WITH animal_revenue AS (
    SELECT
        animal_id AS animal_id,
        observation_date AS observation_date,
        SUM(gross_revenue_discount_amount) AS gross_revenue_discount
    FROM {{ ref('animal_revenue_table')}} 
    GROUP BY 1,2
),

resolved_cases AS (
    SELECT
        resolver_id AS animal_id,
        resolved_date AS observation_date,
        SUM(case_discount_amount) AS case_discount
    FROM {{ ref('resolved_cases_table')}}  
    WHERE resolver_id IS NOT NULL -- Ensure no null IDs
    AND resolved_date IS NOT NULL -- Ensure no null dates
    GROUP BY 1,2
),

resolution_costs AS (
    SELECT
        animal_id AS animal_id,
        observation_date AS observation_date,
        SUM(resolution_cost) AS resolution_cost_total
    FROM {{ ref('resolution_costs_table')}} 
    GROUP BY 1,2
),

feeding_sessions AS (
    SELECT
        caretaker_id AS animal_id,
        CAST(feeding_datetime AS DATE) AS observation_date,
        SUM(feeding_cost) AS feeding_cost_total
    FROM {{ ref('feeding_sessions_table')}} 
    GROUP BY 1,2
)

-- Combine data from multiple sources using FULL OUTER JOIN
SELECT * 
FROM animal_revenue
FULL OUTER JOIN resolved_cases
USING(animal_id, observation_date)
FULL OUTER JOIN resolution_costs
USING(animal_id, observation_date)
FULL OUTER JOIN feeding_sessions
USING(animal_id, observation_date)

-- Apply incremental or full-refresh logic based on the context
WHERE
{% if is_incremental() %}

{{ custom_incremental_macro(                          
    "observation_date",
    "DATE_SUB(CURRENT_DATE(), INTERVAL 2 MONTH)",
    "CURRENT_DATE()",
    "DATE_SUB(CURRENT_DATE(), INTERVAL 5 YEAR)",
    "CURRENT_DATE()",
    "EXTRACT(DAYOFWEEK FROM CURRENT_DATE()) = 6") }}

{% else %}

    -- Full Refresh Logic
    observation_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 5 YEAR)

{% endif %}
    
{%- if var('test_run') == true %}
LIMIT {{ var('test_query_limit') | as_number }}
{%- endif %}
