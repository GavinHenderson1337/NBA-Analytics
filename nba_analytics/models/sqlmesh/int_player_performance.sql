-- SQLMesh Intermediate Model: Player Performance
-- This model combines and cleans player statistics with advanced metrics

{{ config(
    materialized='table',
    partition_by={'field': 'data_processed_at', 'data_type': 'timestamp'},
    cluster_by=['player_id', 'team_id', 'season']
) }}

-- Combine staging data sources
WITH player_base AS (
    SELECT 
        player_id,
        player_name,
        team_id,
        standardized_team_name as team_name,
        season,
        is_active
    FROM {{ ref('stg_nba_players') }}
    WHERE is_active = TRUE  -- Only include active players
),

-- Aggregate game-level statistics to season level
game_stats_aggregated AS (
    SELECT 
        player_id,
        COUNT(*) as games_played,
        AVG(minutes) as avg_minutes_per_game,
        AVG(pts) as avg_points_per_game,
        AVG(reb) as avg_rebounds_per_game,
        AVG(ast) as avg_assists_per_game,
        AVG(stl) as avg_steals_per_game,
        AVG(blk) as avg_blocks_per_game,
        AVG(tov) as avg_turnovers_per_game,
        
        -- Shooting percentages
        SAFE_DIVIDE(SUM(fgm), SUM(fga)) as field_goal_pct,
        SAFE_DIVIDE(SUM(fg3m), SUM(fg3a)) as three_point_pct,
        SAFE_DIVIDE(SUM(ftm), SUM(fta)) as free_throw_pct,
        
        -- Plus/minus average
        AVG(plus_minus) as avg_plus_minus,
        
        -- Win/Loss record
        SUM(CASE WHEN wl = 'W' THEN 1 ELSE 0 END) as wins,
        SUM(CASE WHEN wl = 'L' THEN 1 ELSE 0 END) as losses
        
    FROM {{ ref('stg_nba_player_stats') }}
    GROUP BY player_id
),

-- Get advanced statistics
advanced_stats AS (
    SELECT 
        player_id,
        gp as games_played_advanced,
        min as minutes_per_game_advanced,
        off_rating as offensive_rating,
        def_rating as defensive_rating,
        net_rating,
        ast_pct as assist_percentage,
        reb_pct as rebound_percentage,
        usg_pct as usage_rate,
        ts_pct as true_shooting_pct,
        efg_pct as effective_fg_pct,
        pie as player_impact_estimate
    FROM {{ ref('stg_nba_advanced_stats') }}
),

-- Calculate derived metrics
derived_metrics AS (
    SELECT 
        gs.player_id,
        
        -- Use advanced stats when available, fallback to game stats
        COALESCE(gs.games_played, as.games_played_advanced) as games_played,
        COALESCE(gs.avg_minutes_per_game, as.minutes_per_game_advanced) as minutes_per_game,
        gs.avg_points_per_game as points_per_game,
        gs.avg_rebounds_per_game as rebounds_per_game,
        gs.avg_assists_per_game as assists_per_game,
        gs.avg_steals_per_game as steals_per_game,
        gs.avg_blocks_per_game as blocks_per_game,
        gs.avg_turnovers_per_game as turnovers_per_game,
        
        -- Shooting percentages
        gs.field_goal_pct,
        gs.three_point_pct,
        gs.free_throw_pct,
        
        -- Advanced metrics
        as.offensive_rating,
        as.defensive_rating,
        as.net_rating,
        as.assist_percentage,
        as.rebound_percentage,
        as.usage_rate,
        as.true_shooting_pct,
        as.effective_fg_pct,
        as.player_impact_estimate,
        
        -- Calculate Player Efficiency Rating (PER) - simplified version
        -- PER = (Points + 3PM*0.5 + (2-FG%)*FGM + FT*0.5*(2-FG%) + AST - TOV - (FGA-FGM) - (FTA-FTM)*0.5)
        SAFE_DIVIDE(
            gs.avg_points_per_game + 
            (gs.avg_points_per_game * 0.1) * 0.5 +  -- Estimate 3PM from points
            (2 - COALESCE(gs.field_goal_pct, 0.45)) * (gs.avg_points_per_game * 0.4) +  -- Estimate FGM
            (gs.avg_points_per_game * 0.2) * 0.5 * (2 - COALESCE(gs.field_goal_pct, 0.45)) +  -- Estimate FTM
            gs.avg_assists_per_game - 
            gs.avg_turnovers_per_game - 
            (gs.avg_points_per_game * 0.4) * 0.5 -  -- Estimate missed FGs
            (gs.avg_points_per_game * 0.2) * 0.5 * 0.5,  -- Estimate missed FTs
            COALESCE(gs.avg_minutes_per_game, 25)
        ) * 48 as player_efficiency_rating,
        
        -- Calculate Win Shares per 48 minutes (simplified)
        SAFE_DIVIDE(
            gs.avg_points_per_game + gs.avg_rebounds_per_game + gs.avg_assists_per_game + 
            gs.avg_steals_per_game + gs.avg_blocks_per_game - gs.avg_turnovers_per_game,
            COALESCE(gs.avg_minutes_per_game, 25)
        ) * 48 as win_shares_per_48,
        
        -- Win percentage
        SAFE_DIVIDE(gs.wins, gs.wins + gs.losses) as win_pct
        
    FROM game_stats_aggregated gs
    LEFT JOIN advanced_stats as ON gs.player_id = as.player_id
),

-- Final output
final AS (
    SELECT 
        pb.player_id,
        pb.player_name,
        pb.team_id,
        pb.team_name,
        pb.season,
        dm.games_played,
        dm.minutes_per_game,
        dm.points_per_game,
        dm.rebounds_per_game,
        dm.assists_per_game,
        dm.steals_per_game,
        dm.blocks_per_game,
        dm.turnovers_per_game,
        dm.field_goal_pct,
        dm.three_point_pct,
        dm.free_throw_pct,
        dm.player_efficiency_rating,
        dm.true_shooting_pct,
        dm.effective_fg_pct,
        dm.usage_rate,
        dm.win_shares_per_48,
        dm.offensive_rating,
        dm.defensive_rating,
        dm.net_rating,
        dm.player_impact_estimate,
        dm.win_pct,
        CURRENT_TIMESTAMP() as data_processed_at
        
    FROM player_base pb
    LEFT JOIN derived_metrics dm ON pb.player_id = dm.player_id
    WHERE dm.games_played >= 10  -- Only include players with meaningful playing time
)

SELECT * FROM final
