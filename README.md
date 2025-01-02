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


