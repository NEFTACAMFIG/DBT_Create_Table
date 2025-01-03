# Incremental Model for Animal Data Management in DBT

## Overview
This DBT model is designed to manage and analyze animal-related data using an incremental approach. It processes data from multiple sources, combines it, and handles schema changes dynamically to ensure accurate and efficient data management.

## Key Features
- **Incremental Materialization**: The model uses the `insert_overwrite` strategy to update only the necessary partitions, reducing processing time.
- **Partitioning by Date**: Data is partitioned by the `observation_date` column, with daily granularity.
- **Dynamic Schema Management**: Automatically syncs column changes using the `on_schema_change='sync_all_columns'` configuration.
- **Data Sources**: Combines data from multiple sources (e.g., animal revenue, resolved cases, resolution costs, and feeding sessions) using `FULL OUTER JOIN`.

## Data Sources
The model processes data from the following sources:

1. **Animal Revenue**: Tracks revenue data associated with animals, including gross revenue discounts.
2. **Resolved Cases**: Logs cases resolved by caretakers, including discount amounts.
3. **Resolution Costs**: Calculates total costs associated with resolving issues.
4. **Feeding Sessions**: Summarizes costs associated with feeding sessions for each animal.

## Logic
The model uses Common Table Expressions (CTEs) to preprocess data from each source. These CTEs are then combined using `FULL OUTER JOIN` to create a unified dataset. The logic ensures that null values in critical fields (e.g., `animal_id` and `observation_date`) are filtered out.

### Incremental vs. Full Refresh
- **Incremental Run**: Processes data from the past two months and updates partitions accordingly.
- **Full Refresh**: Processes all data from the past five years.

## Configuration Parameters
- **Partition Field**: `observation_date`
- **Incremental Macro**: A custom macro ensures the correct date ranges are applied during incremental runs.
- **Testing Mode**: Includes a limit for test runs using `test_query_limit`.

## How to Use
1. Place this model in your DBT project's `models` directory.
2. Replace the placeholder table names (`animal_revenue_table`, `resolved_cases_table`, etc.) with the actual table references in your data warehouse.
3. Adjust the date ranges and other parameters to fit your use case.
4. Run the model using DBT commands:
   - `dbt run` for a standard run.
   - `dbt run --full-refresh` for a full refresh.


## Future Enhancements
- Add more granular partitioning based on additional fields (e.g., species, region).
- Include automated testing for data quality.
- Expand data sources to include more operational metrics.

# Wildlife Analytics SQL Query

## Overview
This query processes and analyzes wildlife monitoring data from various habitats. It identifies trends in species behavior, habitat usage, and logistical activities. The script is designed to aggregate event details, calculate key metrics, and highlight the top species, suppliers, observation stations, and logistics partners.

## Key Features
- **Data Aggregation**: Combines data from multiple sources, including event logs, habitats, and supplier information.
- **Dynamic Metrics Calculation**: Calculates event counts and food consumption for species across regions.
- **Filtering**: Focuses on specific regions, species, and event categories for precise analysis.
- **Top Entities**: Highlights the top 10 species, suppliers, observation stations, and logistics partners based on event frequencies.
- **Flexible Timeframe**: Uses a 6-month lookback period for analysis.

## Query Breakdown

### 1. **animal_event_count**
Calculates the total number of events for each habitat by counting specific event types (e.g., "Migration", "Nesting", "Feeding").

### 2. **animal_event_details**
Enriches the event data by:
- Adding dimensional attributes like region, species type, and observation station.
- Cleaning and standardizing event descriptions.
- Calculating key metrics, such as total event count and food consumption.

### 3. **top_species**
Identifies the top 10 animal categories based on the frequency of key events (e.g., "Feeding", "Nesting", "Migration").

### 4. **top_suppliers**
Lists the top 10 suppliers providing resources (e.g., food) based on event data.

### 5. **top_observation_stations**
Ranks the top 10 observation stations where significant wildlife activity has been recorded.

### 6. **top_logistics_partners**
Highlights the top 10 logistics partners involved in habitat management or event facilitation.

### 7. **Final Query**
Filters the enriched data for:
- Events recorded in the last 6 months.
- Specific region: North America.
- Species type: Mammals.
- Event category: Migration ("MG").
- Supplier: Nature Supply Co.
- Top 10 species based on event data.

## Key Parameters
- **Lookback Period**: The query filters data from the last 6 months (`DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH)`).
- **Regions**: Focused on North America.
- **Species Type**: Mammals.
- **Event Description**: Events classified as "Feeding".
