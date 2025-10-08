-- SQLMesh Staging Model: NBA Players
-- This model ingests raw NBA player data from the API and performs initial cleaning

-- Model configuration
{{ config(
    materialized='incremental',
    unique_key='player_id',
    partition_by={'field': 'data_loaded_at', 'data_type': 'timestamp'},
    cluster_by=['team_id', 'season']
) }}

-- Define the source data (this would typically come from a data source)
-- For this example, we'll use a CTE to simulate the raw data
WITH raw_player_data AS (
    -- This CTE represents the raw data from the NBA API
    -- In a real implementation, this would reference a source table or file
    SELECT 
        player_id,
        player_name,
        team_id,
        team_name,
        season,
        is_active,
        CURRENT_TIMESTAMP() as data_loaded_at
    FROM {{ source('nba_api', 'players') }}
    WHERE 1=1
    {% if is_incremental() %}
        -- Incremental processing: only load new data
        AND data_loaded_at > (SELECT MAX(data_loaded_at) FROM {{ this }})
    {% endif %}
),

-- Data cleaning and transformation
cleaned_player_data AS (
    SELECT 
        -- Primary key and identifiers
        CAST(player_id AS INT64) as player_id,
        TRIM(player_name) as player_name,
        CAST(team_id AS INT64) as team_id,
        TRIM(team_name) as team_name,
        
        -- Season information
        season,
        CASE 
            WHEN is_active = 1 THEN TRUE
            WHEN is_active = 0 THEN FALSE
            ELSE FALSE
        END as is_active,
        
        -- Metadata
        data_loaded_at,
        
        -- Data quality flags
        CASE 
            WHEN player_id IS NULL OR player_name IS NULL OR team_id IS NULL THEN FALSE
            ELSE TRUE
        END as is_valid_record
        
    FROM raw_player_data
),

-- Final output with data quality checks
final AS (
    SELECT 
        player_id,
        player_name,
        team_id,
        team_name,
        season,
        is_active,
        data_loaded_at,
        
        -- Add derived fields
        CASE 
            WHEN team_name LIKE '%Lakers%' OR team_name LIKE '%LAL%' THEN 'Los Angeles Lakers'
            WHEN team_name LIKE '%Warriors%' OR team_name LIKE '%GSW%' THEN 'Golden State Warriors'
            WHEN team_name LIKE '%Celtics%' OR team_name LIKE '%BOS%' THEN 'Boston Celtics'
            WHEN team_name LIKE '%Heat%' OR team_name LIKE '%MIA%' THEN 'Miami Heat'
            WHEN team_name LIKE '%Nets%' OR team_name LIKE '%BKN%' THEN 'Brooklyn Nets'
            ELSE team_name
        END as standardized_team_name,
        
        -- Player name standardization
        CASE 
            WHEN player_name LIKE '%,%' THEN 
                TRIM(SPLIT(player_name, ',')[SAFE_OFFSET(1)]) || ' ' || TRIM(SPLIT(player_name, ',')[SAFE_OFFSET(0)])
            ELSE player_name
        END as standardized_player_name
        
    FROM cleaned_player_data
    WHERE is_valid_record = TRUE  -- Only include valid records
)

SELECT * FROM final
