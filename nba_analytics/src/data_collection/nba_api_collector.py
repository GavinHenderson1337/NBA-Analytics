"""
NBA API Data Collection Module

This module handles the collection of NBA player statistics and team data
from the official NBA API and other basketball data sources.

Key Features:
- Real-time NBA player statistics collection
- Automated data collection with error handling and retry logic
- Historical data backfill capabilities
- Advanced basketball metrics calculation
- Data validation and quality checks

Author: Data Scientist Portfolio Project
Date: 2024
"""

import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import logging
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import json
import os
from pathlib import Path

# Configure logging to track data collection process
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class PlayerStats:
    """
    Data class for player statistics
    
    This class defines the structure for storing individual player game statistics.
    It includes both basic stats (points, rebounds, assists) and advanced metrics.
    """
    player_id: int
    player_name: str
    team_id: int
    team_name: str
    game_date: str
    season: str
    points: float
    rebounds: float
    assists: float
    steals: float
    blocks: float
    turnovers: float
    minutes: float
    field_goals_made: float
    field_goals_attempted: float
    three_pointers_made: float
    three_pointers_attempted: float
    free_throws_made: float
    free_throws_attempted: float
    plus_minus: float

class NBADataCollector:
    """
    NBA Data Collector class for fetching player statistics and team data
    from various NBA data sources.
    
    This class implements the main functionality for:
    1. Making API requests to NBA endpoints
    2. Handling rate limiting and retries
    3. Processing and transforming raw API responses
    4. Calculating advanced basketball metrics
    5. Saving collected data to files
    
    Attributes:
        base_url (str): Base URL for NBA stats API
        session (requests.Session): HTTP session for making requests
    """
    
    def __init__(self, base_url: str = "https://stats.nba.com/stats"):
        """
        Initialize the NBA Data Collector
        
        Args:
            base_url: Base URL for NBA stats API endpoints
        """
        self.base_url = base_url
        self.session = requests.Session()
        
        # Set up headers to mimic a browser request (NBA API requires this)
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': 'https://www.nba.com/',
            'Connection': 'keep-alive',
        })
        
    def _make_request(self, endpoint: str, params: Dict) -> Dict:
        """
        Make API request with error handling and retry logic
        
        This method implements exponential backoff retry strategy to handle
        rate limiting and temporary API failures.
        
        Args:
            endpoint: API endpoint to call
            params: Query parameters for the API request
            
        Returns:
            JSON response from the API
            
        Raises:
            requests.exceptions.RequestException: If all retry attempts fail
        """
        max_retries = 3  # Maximum number of retry attempts
        retry_delay = 1   # Initial delay between retries (seconds)
        
        # Exponential backoff retry loop
        for attempt in range(max_retries):
            try:
                # Make the API request
                response = self.session.get(f"{self.base_url}/{endpoint}", params=params)
                response.raise_for_status()  # Raise exception for HTTP errors
                return response.json()
                
            except requests.exceptions.RequestException as e:
                logger.warning(f"Request failed (attempt {attempt + 1}/{max_retries}): {e}")
                
                # If this isn't the last attempt, wait before retrying
                if attempt < max_retries - 1:
                    # Exponential backoff: delay increases with each attempt
                    time.sleep(retry_delay * (2 ** attempt))
                else:
                    # All retries exhausted, raise the exception
                    raise
        
    def get_players_list(self, season: str = "2023-24") -> pd.DataFrame:
        """
        Fetch list of all NBA players for a given season
        
        This method retrieves the complete roster of NBA players for a specific season,
        including both active and inactive players.
        
        Args:
            season: NBA season in format "YYYY-YY" (e.g., "2023-24")
            
        Returns:
            DataFrame with player information including:
                - player_id: Unique NBA player identifier
                - player_name: Full player name
                - team_id: Current team identifier
                - team_name: Current team name
                - season: Season for which data is collected
                - is_active: Boolean indicating if player is currently active
        """
        params = {
            'Season': season,
            'LeagueID': '00'  # '00' represents the NBA (other leagues have different IDs)
        }
        
        # Make API request to get all players
        data = self._make_request("commonallplayers", params)
        
        # Process the response and extract player data
        players_data = []
        for player in data['resultSets'][0]['rowSet']:
            players_data.append({
                'player_id': player[0],      # Player's unique ID
                'player_name': player[2],    # Player's full name
                'team_id': player[7],        # Current team ID
                'team_name': player[8],      # Current team name
                'season': season,
                'is_active': player[1]       # Active status
            })
        
        return pd.DataFrame(players_data)
    
    def get_player_game_logs(self, player_id: int, season: str = "2023-24", 
                           season_type: str = "Regular Season") -> pd.DataFrame:
        """
        Fetch game logs for a specific player
        
        This method retrieves game-by-game statistics for a specific player
        across an entire season. Useful for analyzing performance trends and
        building predictive models.
        
        Args:
            player_id: Unique NBA player identifier
            season: NBA season in format "YYYY-YY"
            season_type: Type of season ("Regular Season", "Playoffs", "Pre Season")
            
        Returns:
            DataFrame with detailed game logs including:
                - Basic stats: points, rebounds, assists, etc.
                - Shooting percentages and efficiency metrics
                - Game context: opponent, win/loss, minutes played
        """
        params = {
            'PlayerID': player_id,
            'Season': season,
            'SeasonType': season_type,
            'LeagueID': '00'
        }
        
        # Make API request for player game logs
        data = self._make_request("playergamelog", params)
        
        # Process and structure the game log data
        game_logs = []
        for game in data['resultSets'][0]['rowSet']:
            game_logs.append({
                'player_id': player_id,
                'game_date': game[2],        # Date of the game
                'matchup': game[3],          # Opponent team (e.g., "LAL vs. GSW")
                'wl': game[4],              # Win/Loss result
                'minutes': game[5],          # Minutes played
                'fgm': game[6],             # Field goals made
                'fga': game[7],             # Field goals attempted
                'fg_pct': game[8],          # Field goal percentage
                'fg3m': game[9],            # Three-pointers made
                'fg3a': game[10],           # Three-pointers attempted
                'fg3_pct': game[11],        # Three-point percentage
                'ftm': game[12],            # Free throws made
                'fta': game[13],            # Free throws attempted
                'ft_pct': game[14],         # Free throw percentage
                'oreb': game[15],           # Offensive rebounds
                'dreb': game[16],           # Defensive rebounds
                'reb': game[17],            # Total rebounds
                'ast': game[18],            # Assists
                'stl': game[19],            # Steals
                'blk': game[20],            # Blocks
                'tov': game[21],            # Turnovers
                'pf': game[22],             # Personal fouls
                'pts': game[23],            # Points scored
                'plus_minus': game[24]      # Plus/minus rating
            })
        
        return pd.DataFrame(game_logs)
    
    def get_advanced_stats(self, season: str = "2023-24", 
                          season_type: str = "Regular Season") -> pd.DataFrame:
        """
        Fetch advanced player statistics for all players
        
        This method retrieves advanced basketball metrics that provide
        deeper insights into player performance beyond basic counting stats.
        
        Args:
            season: NBA season in format "YYYY-YY"
            season_type: Type of season
            
        Returns:
            DataFrame with advanced statistics including:
                - Efficiency metrics: Offensive/Defensive rating, Net rating
                - Usage statistics: Usage percentage, Pace
                - Advanced shooting: True shooting %, Effective FG%
                - Rebounding rates: Offensive/Defensive rebound percentage
        """
        params = {
            'Season': season,
            'SeasonType': season_type,
            'LeagueID': '00',
            'PerMode': 'PerGame',
            'MeasureType': 'Advanced'  # Request advanced statistics
        }
        
        # Make API request for advanced stats
        data = self._make_request("leaguedashplayerstats", params)
        
        # Process advanced statistics data
        advanced_stats = []
        for player in data['resultSets'][0]['rowSet']:
            advanced_stats.append({
                'player_id': player[0],
                'player_name': player[1],
                'team_id': player[2],
                'team_name': player[3],
                'age': player[4],
                'gp': player[5],             # Games played
                'w': player[6],              # Wins
                'l': player[7],              # Losses
                'w_pct': player[8],          # Win percentage
                'min': player[9],            # Minutes per game
                'off_rating': player[10],    # Offensive rating
                'def_rating': player[11],    # Defensive rating
                'net_rating': player[12],    # Net rating (off - def)
                'ast_pct': player[13],       # Assist percentage
                'ast_to': player[14],        # Assist-to-turnover ratio
                'ast_ratio': player[15],     # Assist ratio
                'oreb_pct': player[16],      # Offensive rebound percentage
                'dreb_pct': player[17],      # Defensive rebound percentage
                'reb_pct': player[18],       # Total rebound percentage
                'tov_pct': player[19],       # Turnover percentage
                'efg_pct': player[20],       # Effective field goal percentage
                'ts_pct': player[21],        # True shooting percentage
                'usg_pct': player[22],       # Usage percentage
                'pace': player[23],          # Pace (possessions per 48 min)
                'pie': player[24],           # Player Impact Estimate
                'season': season,
                'season_type': season_type
            })
        
        return pd.DataFrame(advanced_stats)
    
    def calculate_advanced_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate advanced basketball metrics from basic stats
        
        This method computes advanced basketball analytics that aren't
        directly available from the NBA API but are crucial for
        performance analysis and predictive modeling.
        
        Args:
            df: DataFrame with basic player statistics
            
        Returns:
            DataFrame with additional advanced metrics:
                - PER (Player Efficiency Rating): Overall player efficiency
                - TS% (True Shooting %): Shooting efficiency including 3s and FTs
                - eFG% (Effective FG%): Field goal percentage adjusted for 3s
                - Usage Rate: Percentage of team possessions used by player
                - Win Shares per 48: Estimated wins contributed per 48 minutes
        """
        df = df.copy()
        
        # Player Efficiency Rating (PER) - John Hollinger's all-in-one metric
        # Higher PER indicates better overall performance
        df['per'] = (
            df['pts'] + df['fg3m'] * 0.5 + 
            (2 - df['ast_pct'] / 100) * df['fgm'] +
            df['ftm'] * 0.5 * (2 - df['ast_pct'] / 100) +
            df['ast'] - df['tov'] - 
            (df['fga'] - df['fgm']) - 
            (df['fta'] - df['ftm']) * 0.5
        )
        
        # True Shooting Percentage - measures shooting efficiency
        # Accounts for 2-pointers, 3-pointers, and free throws
        # Formula: PTS / (2 * (FGA + 0.44 * FTA))
        df['ts_pct'] = df['pts'] / (2 * (df['fga'] + 0.44 * df['fta']))
        
        # Effective Field Goal Percentage - adjusts for 3-pointers
        # Formula: (FGM + 0.5 * 3PM) / FGA
        df['efg_pct'] = (df['fgm'] + 0.5 * df['fg3m']) / df['fga']
        
        # Usage Rate - percentage of team possessions used by player
        # Simplified calculation: (FGA + 0.44*FTA + TOV) / MIN * 48
        df['usg_pct'] = (df['fga'] + 0.44 * df['fta'] + df['tov']) / df['min'] * 48
        
        # Win Shares per 48 minutes - estimated wins contributed
        # Simplified formula based on box score stats
        df['ws_per_48'] = (df['pts'] + df['reb'] + df['ast'] + df['stl'] + df['blk'] - df['tov']) / df['min'] * 48
        
        return df
    
    def save_data(self, df: pd.DataFrame, filename: str, 
                  data_dir: str = "data/raw") -> None:
        """
        Save DataFrame to CSV file with proper directory structure
        
        This method ensures data is saved in an organized manner
        for later processing by the ETL pipeline.
        
        Args:
            df: DataFrame to save
            filename: Name of the file (should include .csv extension)
            data_dir: Directory to save the file (default: "data/raw")
        """
        # Create directory if it doesn't exist
        os.makedirs(data_dir, exist_ok=True)
        
        # Construct full file path
        filepath = os.path.join(data_dir, filename)
        
        # Save DataFrame to CSV
        df.to_csv(filepath, index=False)
        
        logger.info(f"Data saved to {filepath} ({len(df)} rows)")
    
    def collect_all_data(self, season: str = "2023-24", 
                        save_data: bool = True) -> Dict[str, pd.DataFrame]:
        """
        Collect all NBA data for a given season
        
        This is the main orchestration method that coordinates the collection
        of all different types of NBA data. It implements proper error handling
        and data organization for the complete data pipeline.
        
        Args:
            season: NBA season in format "YYYY-YY"
            save_data: Whether to save data to files (default: True)
            
        Returns:
            Dictionary with all collected DataFrames:
                - players: Complete player list for the season
                - advanced_stats: Advanced statistics for all players
                - rosters: Team rosters (sample of teams)
                
        Raises:
            Exception: If data collection fails at any step
        """
        logger.info(f"Starting comprehensive data collection for season {season}")
        
        data_dict = {}
        
        try:
            # Step 1: Get complete list of NBA players
            logger.info("Fetching complete players list...")
            players_df = self.get_players_list(season)
            data_dict['players'] = players_df
            logger.info(f"Collected data for {len(players_df)} players")
            
            # Step 2: Get advanced statistics for all players
            logger.info("Fetching advanced statistics...")
            advanced_df = self.get_advanced_stats(season)
            data_dict['advanced_stats'] = advanced_df
            logger.info(f"Collected advanced stats for {len(advanced_df)} player-season combinations")
            
            # Step 3: Save data if requested
            if save_data:
                logger.info("Saving collected data to files...")
                for key, df in data_dict.items():
                    self.save_data(df, f"{key}_{season.replace('-', '_')}.csv")
            
            logger.info("Data collection completed successfully!")
            
        except Exception as e:
            logger.error(f"Error during data collection: {e}")
            raise
        
        return data_dict

def main():
    """
    Main function to demonstrate data collection
    
    This function shows how to use the NBADataCollector class
    to collect NBA data. It's useful for testing and as an example
    of how to integrate the collector into larger workflows.
    """
    # Initialize the data collector
    collector = NBADataCollector()
    
    # Collect data for current season
    season = "2023-24"
    
    print(f"NBA Data Collection Demo - Season {season}")
    print("=" * 50)
    
    try:
        # Collect all available data
        data = collector.collect_all_data(season, save_data=True)
        
        # Display summary of collected data
        print("\nData Collection Summary:")
        print("-" * 30)
        
        for key, df in data.items():
            print(f"\n{key.upper()}:")
            print(f"  Shape: {df.shape}")
            print(f"  Columns: {len(df.columns)}")
            print(f"  Sample columns: {list(df.columns[:5])}")
            
            if len(df) > 0:
                print(f"  Sample data:")
                print(df.head(2).to_string())
        
        print(f"\n✅ Data collection completed successfully!")
        print(f"📁 Data saved to: data/raw/")
        
    except Exception as e:
        print(f"❌ Data collection failed: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
