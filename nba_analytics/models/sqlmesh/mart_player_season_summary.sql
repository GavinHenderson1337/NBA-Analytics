-- SQLMesh Mart Model: Player Season Summary
-- This is the final business-ready table with comprehensive player analytics

{{ config(
    materialized='table',
    partition_by={'field': 'data_created_at', 'data_type': 'timestamp'},
    cluster_by=['player_id', 'team_id', 'season', 'performance_tier']
) }}

-- Base player performance data
WITH player_performance AS (
    SELECT * FROM {{ ref('int_player_performance') }}
),

-- Calculate position classification based on player statistics
position_classification AS (
    SELECT 
        *,
        CASE 
            -- Point Guards: High assists, moderate points, lower rebounds
            WHEN assists_per_game >= 5.0 AND rebounds_per_game < 8.0 AND points_per_game < 25.0 THEN 'Point Guard'
            -- Shooting Guards: High scoring, moderate assists and rebounds
            WHEN points_per_game >= 18.0 AND assists_per_game < 5.0 AND rebounds_per_game < 7.0 THEN 'Shooting Guard'
            -- Small Forwards: Balanced scoring and rebounding, moderate assists
            WHEN points_per_game >= 12.0 AND rebounds_per_game >= 5.0 AND rebounds_per_game < 10.0 AND assists_per_game < 6.0 THEN 'Small Forward'
            -- Power Forwards: High rebounds, good scoring, some assists
            WHEN rebounds_per_game >= 8.0 AND points_per_game >= 10.0 AND assists_per_game < 5.0 THEN 'Power Forward'
            -- Centers: Very high rebounds, good scoring, low assists
            WHEN rebounds_per_game >= 10.0 AND points_per_game >= 8.0 AND assists_per_game < 4.0 THEN 'Center'
            -- Utility/Combo players: High assists and good rebounding
            WHEN assists_per_game >= 6.0 AND rebounds_per_game >= 6.0 THEN 'Combo Forward/Guard'
            ELSE 'Utility Player'
        END as primary_position,
        
        -- Position tier classification
        CASE 
            WHEN primary_position IN ('Point Guard', 'Shooting Guard') THEN 'Backcourt'
            WHEN primary_position IN ('Small Forward', 'Power Forward') THEN 'Frontcourt'
            WHEN primary_position = 'Center' THEN 'Big Man'
            ELSE 'Utility'
        END as position_tier
        
    FROM player_performance
),

-- Calculate performance tier classification
performance_tier_classification AS (
    SELECT 
        *,
        -- Calculate composite performance score (0-100)
        LEAST(100, GREATEST(0, 
            (points_per_game * 2.5) + 
            (rebounds_per_game * 1.5) + 
            (assists_per_game * 2.0) + 
            (steals_per_game * 3.0) + 
            (blocks_per_game * 3.0) - 
            (turnovers_per_game * 1.5) +
            (player_efficiency_rating * 0.5) +
            (true_shooting_pct * 50) +
            (win_shares_per_48 * 10)
        )) as performance_score,
        
        -- Performance tier based on composite score and PER
        CASE 
            WHEN (player_efficiency_rating >= 25 AND points_per_game >= 20) OR 
                 (player_efficiency_rating >= 30) THEN 'Elite'
            WHEN (player_efficiency_rating >= 20 AND points_per_game >= 15) OR 
                 (player_efficiency_rating >= 25 AND points_per_game >= 12) THEN 'All-Star'
            WHEN (player_efficiency_rating >= 15 AND points_per_game >= 10) OR 
                 (player_efficiency_rating >= 18 AND minutes_per_game >= 20) THEN 'Starter'
            WHEN (player_efficiency_rating >= 10 AND minutes_per_game >= 10) THEN 'Rotation Player'
            ELSE 'Bench Player'
        END as performance_tier,
        
        -- Injury risk score (simplified calculation based on usage and age factors)
        LEAST(100, GREATEST(0,
            20 +  -- Base risk
            (usage_rate * 0.5) +  -- Higher usage = higher risk
            (minutes_per_game * 0.3) +  -- More minutes = higher risk
            (games_played * -0.2) +  -- More games played = lower risk (health indicator)
            (CASE WHEN player_efficiency_rating < 10 THEN 15 ELSE 0 END)  -- Low performance = potential injury
        )) as injury_risk_score
        
    FROM position_classification
),

-- Calculate rankings
rankings AS (
    SELECT 
        *,
        -- Overall season ranking
        ROW_NUMBER() OVER (ORDER BY performance_score DESC) as season_rank,
        
        -- Position-specific ranking
        ROW_NUMBER() OVER (
            PARTITION BY primary_position 
            ORDER BY performance_score DESC
        ) as position_rank,
        
        -- Team ranking
        ROW_NUMBER() OVER (
            PARTITION BY team_id 
            ORDER BY performance_score DESC
        ) as team_rank
        
    FROM performance_tier_classification
),

-- Final output with additional derived fields
final AS (
    SELECT 
        -- Primary identifiers
        player_id,
        player_name,
        team_id,
        team_name,
        season,
        
        -- Classification
        primary_position,
        position_tier,
        performance_tier,
        
        -- Core statistics
        games_played,
        minutes_per_game,
        points_per_game,
        rebounds_per_game,
        assists_per_game,
        
        -- Advanced metrics
        player_efficiency_rating,
        true_shooting_pct,
        usage_rate,
        win_shares_per_48,
        net_rating,
        
        -- Calculated scores
        performance_score,
        injury_risk_score,
        
        -- Rankings
        season_rank,
        position_rank,
        team_rank,
        
        -- Additional metrics
        steals_per_game,
        blocks_per_game,
        turnovers_per_game,
        field_goal_pct,
        three_point_pct,
        free_throw_pct,
        offensive_rating,
        defensive_rating,
        player_impact_estimate,
        win_pct,
        
        -- Metadata
        CURRENT_TIMESTAMP() as data_created_at,
        CURRENT_TIMESTAMP() as data_updated_at
        
    FROM rankings
)

SELECT * FROM final
