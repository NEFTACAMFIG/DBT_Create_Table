WITH animal_event_count AS (
  SELECT
    habitat_id,
    COUNT(event_type) AS total_events
  FROM `wildlife_analytics.event_logs` e, UNNEST(events) AS event
  WHERE event.event_type IN ('Migration', 'Nesting', 'Feeding')
  GROUP BY 1
),

animal_event_details AS (
  SELECT
    -- Dimensional attributes
    observation_date AS date_recorded,
    region AS habitat_region,
    animal_classification AS species_type,
    event_category AS event_category_short,
    CASE 
      WHEN event_description = "Migration" THEN event_type 
      ELSE event_description 
    END AS event_description_cleaned,
    food_supplier AS main_supplier,
    animal_type AS category_name,
    observation_station AS observation_point,
    logistics_partner AS logistics_provider,
    -- Metric calculations
    c.total_events AS total_event_count,
    food_quantity_consumed AS food_used
  FROM `wildlife_analytics.event_logs` e, UNNEST(events) u
  LEFT JOIN animal_event_count c ON e.habitat_id = c.habitat_id
),

top_species AS (
  SELECT
    category_name
  FROM animal_event_details
  WHERE species_type IS NOT NULL
    AND event_description_cleaned IN ('Feeding', 'Nesting', 'Migration', 'Territorial Disputes')
  GROUP BY category_name
  ORDER BY COUNT(event_description_cleaned) DESC
  LIMIT 10
),

top_suppliers AS (
  SELECT
    main_supplier
  FROM animal_event_details
  WHERE species_type IS NOT NULL
    AND event_description_cleaned IN ('Feeding', 'Nesting', 'Migration', 'Territorial Disputes')
  GROUP BY main_supplier
  ORDER BY COUNT(event_description_cleaned) DESC
  LIMIT 10
),

top_observation_stations AS (
  SELECT
    observation_point
  FROM animal_event_details
  WHERE species_type IS NOT NULL
    AND event_description_cleaned IN ('Feeding', 'Nesting', 'Migration', 'Territorial Disputes')
  GROUP BY observation_point
  ORDER BY COUNT(event_description_cleaned) DESC
  LIMIT 10
),

top_logistics_partners AS (
  SELECT
    logistics_provider
  FROM animal_event_details
  WHERE species_type IS NOT NULL
    AND event_description_cleaned IN ('Feeding', 'Nesting', 'Migration', 'Territorial Disputes')
  GROUP BY logistics_provider
  ORDER BY COUNT(event_description_cleaned) DESC
  LIMIT 10
)

SELECT
  category_name
FROM animal_event_details
WHERE date_recorded > DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH)
  AND date_recorded < CURRENT_DATE()
  AND habitat_region = 'North America'
  AND species_type = 'Mammals'
  AND event_category_short = 'MG'
  AND event_description_cleaned = 'Feeding'
  AND main_supplier = 'Nature Supply Co.'
  AND category_name IN (SELECT category_name FROM top_species)
GROUP BY 1;
