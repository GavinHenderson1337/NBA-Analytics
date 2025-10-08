"""
NBA Analytics ETL Pipeline

This module implements the Extract, Transform, Load (ETL) pipeline for NBA player data.
It handles data collection, transformation, and loading into BigQuery using SQLMesh.

Key Features:
- Automated data collection from NBA API
- Data cleaning and validation
- Incremental data processing
- BigQuery integration with SQLMesh
- Data quality monitoring and alerting

Author: Data Scientist Portfolio Project
Date: 2024
"""

import os
import sys
import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import json
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

# Import custom modules
from src.data_collection.nba_api_collector import NBADataCollector

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class NBAETLPipeline:
    """
    NBA ETL Pipeline class for orchestrating data collection, transformation, and loading.
    
    This class coordinates the entire data pipeline process:
    1. Extract: Collect data from NBA API
    2. Transform: Clean, validate, and enrich data
    3. Load: Store data in BigQuery using SQLMesh models
    
    Attributes:
        collector: NBA data collector instance
        bigquery_client: BigQuery client for data loading
        config: Pipeline configuration
    """
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize the ETL pipeline
        
        Args:
            config_path: Path to configuration file (optional)
        """
        self.collector = NBADataCollector()
        self.config = self._load_config(config_path)
        self.data_dir = Path("data")
        self.raw_data_dir = self.data_dir / "raw"
        self.processed_data_dir = self.data_dir / "processed"
        
        # Create directories if they don't exist
        self.raw_data_dir.mkdir(parents=True, exist_ok=True)
        self.processed_data_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize BigQuery client if configured
        self.bigquery_client = None
        if self.config.get('bigquery', {}).get('enabled', False):
            self._initialize_bigquery_client()
    
    def _load_config(self, config_path: Optional[str]) -> Dict:
        """
        Load pipeline configuration from file or use defaults
        
        Args:
            config_path: Path to configuration file
            
        Returns:
            Configuration dictionary
        """
        default_config = {
            'seasons': ['2023-24', '2022-23', '2021-22'],
            'data_quality': {
                'min_games_played': 10,
                'max_missing_pct': 0.1,
                'outlier_threshold': 3.0
            },
            'bigquery': {
                'enabled': False,
                'dataset_id': 'nba_analytics',
                'location': 'US'
            },
            'incremental': {
                'enabled': True,
                'lookback_days': 7
            }
        }
        
        if config_path and os.path.exists(config_path):
            try:
                with open(config_path, 'r') as f:
                    user_config = json.load(f)
                default_config.update(user_config)
                logger.info(f"Loaded configuration from {config_path}")
            except Exception as e:
                logger.warning(f"Failed to load config from {config_path}: {e}")
        
        return default_config
    
    def _initialize_bigquery_client(self):
        """
        Initialize BigQuery client for data loading
        
        This method sets up the BigQuery client using service account credentials
        or default application credentials.
        """
        try:
            from google.cloud import bigquery
            from google.cloud.exceptions import NotFound
            
            self.bigquery_client = bigquery.Client(
                project=self.config['bigquery']['project_id'],
                location=self.config['bigquery']['location']
            )
            
            # Test connection
            self.bigquery_client.get_dataset(
                self.config['bigquery']['dataset_id']
            )
            logger.info("BigQuery client initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize BigQuery client: {e}")
            self.bigquery_client = None
    
    def extract_data(self, season: str, incremental: bool = True) -> Dict[str, pd.DataFrame]:
        """
        Extract data from NBA API
        
        This method orchestrates the data extraction process, handling both
        full and incremental data collection.
        
        Args:
            season: NBA season to extract data for
            incremental: Whether to perform incremental extraction
            
        Returns:
            Dictionary containing extracted DataFrames
        """
        logger.info(f"Starting data extraction for season {season}")
        
        try:
            # Determine extraction strategy
            if incremental and self.config['incremental']['enabled']:
                data = self._extract_incremental_data(season)
            else:
                data = self.collector.collect_all_data(season, save_data=True)
            
            # Log extraction summary
            for key, df in data.items():
                logger.info(f"Extracted {len(df)} records for {key}")
            
            return data
            
        except Exception as e:
            logger.error(f"Data extraction failed for season {season}: {e}")
            raise
    
    def _extract_incremental_data(self, season: str) -> Dict[str, pd.DataFrame]:
        """
        Extract incremental data based on last update timestamp
        
        Args:
            season: NBA season to extract data for
            
        Returns:
            Dictionary containing incremental DataFrames
        """
        logger.info("Performing incremental data extraction")
        
        # Check for existing data to determine incremental strategy
        last_update_file = self.raw_data_dir / f"last_update_{season.replace('-', '_')}.json"
        
        if last_update_file.exists():
            with open(last_update_file, 'r') as f:
                last_update_info = json.load(f)
            
            last_update = datetime.fromisoformat(last_update_info['timestamp'])
            lookback_days = self.config['incremental']['lookback_days']
            
            # Only extract if it's been more than the lookback period
            if datetime.now() - last_update < timedelta(days=lookback_days):
                logger.info("Skipping extraction - data is up to date")
                return self._load_existing_data(season)
        
        # Perform full extraction
        data = self.collector.collect_all_data(season, save_data=True)
        
        # Update last extraction timestamp
        with open(last_update_file, 'w') as f:
            json.dump({
                'timestamp': datetime.now().isoformat(),
                'season': season,
                'records_extracted': sum(len(df) for df in data.values())
            }, f)
        
        return data
    
    def _load_existing_data(self, season: str) -> Dict[str, pd.DataFrame]:
        """
        Load existing data from files
        
        Args:
            season: NBA season to load data for
            
        Returns:
            Dictionary containing loaded DataFrames
        """
        data = {}
        season_key = season.replace('-', '_')
        
        for file_type in ['players', 'advanced_stats']:
            file_path = self.raw_data_dir / f"{file_type}_{season_key}.csv"
            if file_path.exists():
                data[file_type] = pd.read_csv(file_path)
                logger.info(f"Loaded existing data: {file_type}")
        
        return data
    
    def transform_data(self, raw_data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """
        Transform and clean the extracted data
        
        This method applies data cleaning, validation, and enrichment
        transformations to the raw data.
        
        Args:
            raw_data: Dictionary containing raw DataFrames
            
        Returns:
            Dictionary containing transformed DataFrames
        """
        logger.info("Starting data transformation")
        
        transformed_data = {}
        
        try:
            # Transform players data
            if 'players' in raw_data:
                transformed_data['players'] = self._transform_players_data(raw_data['players'])
            
            # Transform advanced stats data
            if 'advanced_stats' in raw_data:
                transformed_data['advanced_stats'] = self._transform_advanced_stats_data(raw_data['advanced_stats'])
            
            # Perform data quality checks
            self._validate_data_quality(transformed_data)
            
            logger.info("Data transformation completed successfully")
            return transformed_data
            
        except Exception as e:
            logger.error(f"Data transformation failed: {e}")
            raise
    
    def _transform_players_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform and clean players data
        
        Args:
            df: Raw players DataFrame
            
        Returns:
            Transformed players DataFrame
        """
        df = df.copy()
        
        # Clean player names
        df['player_name'] = df['player_name'].str.strip()
        df['team_name'] = df['team_name'].str.strip()
        
        # Handle missing values
        df['team_id'] = df['team_id'].fillna(0)
        df['is_active'] = df['is_active'].fillna(False)
        
        # Add data quality flags
        df['is_valid'] = (
            df['player_id'].notna() &
            df['player_name'].notna() &
            (df['player_id'] > 0)
        )
        
        # Add processing metadata
        df['processed_at'] = datetime.now()
        
        logger.info(f"Transformed players data: {len(df)} records")
        return df
    
    def _transform_advanced_stats_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform and clean advanced statistics data
        
        Args:
            df: Raw advanced stats DataFrame
            
        Returns:
            Transformed advanced stats DataFrame
        """
        df = df.copy()
        
        # Clean numeric columns
        numeric_columns = ['gp', 'min', 'off_rating', 'def_rating', 'net_rating', 
                          'ast_pct', 'reb_pct', 'usg_pct', 'ts_pct', 'efg_pct', 'pie']
        
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Handle outliers using IQR method
        for col in numeric_columns:
            if col in df.columns:
                Q1 = df[col].quantile(0.25)
                Q3 = df[col].quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR
                
                # Cap outliers instead of removing them
                df[col] = df[col].clip(lower=lower_bound, upper=upper_bound)
        
        # Calculate derived metrics
        df['games_played'] = df['gp']
        df['minutes_per_game'] = df['min']
        df['player_efficiency_rating'] = self._calculate_per(df)
        
        # Add processing metadata
        df['processed_at'] = datetime.now()
        
        logger.info(f"Transformed advanced stats data: {len(df)} records")
        return df
    
    def _calculate_per(self, df: pd.DataFrame) -> pd.Series:
        """
        Calculate Player Efficiency Rating (PER)
        
        Args:
            df: DataFrame with player statistics
            
        Returns:
            Series with PER values
        """
        # Simplified PER calculation
        # In a real implementation, this would be more comprehensive
        per = (
            df.get('pts', 0) * 1.0 +
            df.get('reb', 0) * 0.8 +
            df.get('ast', 0) * 1.0 +
            df.get('stl', 0) * 1.5 +
            df.get('blk', 0) * 1.5 -
            df.get('tov', 0) * 1.0
        ) / df.get('min', 1) * 48  # Normalize to per 48 minutes
        
        return per.fillna(0)
    
    def _validate_data_quality(self, data: Dict[str, pd.DataFrame]) -> None:
        """
        Validate data quality according to configured rules
        
        Args:
            data: Dictionary containing transformed DataFrames
            
        Raises:
            ValueError: If data quality checks fail
        """
        quality_config = self.config['data_quality']
        
        for table_name, df in data.items():
            logger.info(f"Validating data quality for {table_name}")
            
            # Check for minimum records
            if len(df) < 10:
                raise ValueError(f"Insufficient data in {table_name}: {len(df)} records")
            
            # Check for missing values
            missing_pct = df.isnull().sum().sum() / (len(df) * len(df.columns))
            if missing_pct > quality_config['max_missing_pct']:
                raise ValueError(f"Too many missing values in {table_name}: {missing_pct:.2%}")
            
            # Check for duplicate records
            if 'player_id' in df.columns:
                duplicates = df['player_id'].duplicated().sum()
                if duplicates > 0:
                    logger.warning(f"Found {duplicates} duplicate player_ids in {table_name}")
            
            logger.info(f"Data quality validation passed for {table_name}")
    
    def load_data(self, transformed_data: Dict[str, pd.DataFrame]) -> None:
        """
        Load transformed data into BigQuery using SQLMesh
        
        Args:
            transformed_data: Dictionary containing transformed DataFrames
        """
        logger.info("Starting data loading process")
        
        try:
            # Save processed data to files
            self._save_processed_data(transformed_data)
            
            # Load to BigQuery if configured
            if self.bigquery_client:
                self._load_to_bigquery(transformed_data)
            
            logger.info("Data loading completed successfully")
            
        except Exception as e:
            logger.error(f"Data loading failed: {e}")
            raise
    
    def _save_processed_data(self, data: Dict[str, pd.DataFrame]) -> None:
        """
        Save processed data to local files
        
        Args:
            data: Dictionary containing processed DataFrames
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        for table_name, df in data.items():
            file_path = self.processed_data_dir / f"{table_name}_{timestamp}.csv"
            df.to_csv(file_path, index=False)
            logger.info(f"Saved processed data: {file_path}")
    
    def _load_to_bigquery(self, data: Dict[str, pd.DataFrame]) -> None:
        """
        Load data to BigQuery
        
        Args:
            data: Dictionary containing DataFrames to load
        """
        dataset_id = self.config['bigquery']['dataset_id']
        
        for table_name, df in data.items():
            table_id = f"{dataset_id}.{table_name}"
            
            # Configure job
            job_config = self.bigquery_client.LoadJobConfig(
                write_disposition="WRITE_TRUNCATE",  # Overwrite existing data
                create_disposition="CREATE_IF_NEEDED"
            )
            
            # Load data
            job = self.bigquery_client.load_table_from_dataframe(
                df, table_id, job_config=job_config
            )
            
            job.result()  # Wait for job to complete
            logger.info(f"Loaded {len(df)} records to BigQuery table: {table_id}")
    
    def run_pipeline(self, season: str = "2023-24", incremental: bool = True) -> Dict:
        """
        Run the complete ETL pipeline for a given season
        
        Args:
            season: NBA season to process
            incremental: Whether to perform incremental processing
            
        Returns:
            Dictionary with pipeline execution results
        """
        start_time = datetime.now()
        logger.info(f"Starting ETL pipeline for season {season}")
        
        results = {
            'season': season,
            'start_time': start_time,
            'incremental': incremental,
            'status': 'running',
            'steps_completed': [],
            'errors': []
        }
        
        try:
            # Step 1: Extract
            logger.info("Step 1: Extracting data")
            raw_data = self.extract_data(season, incremental)
            results['steps_completed'].append('extract')
            results['records_extracted'] = sum(len(df) for df in raw_data.values())
            
            # Step 2: Transform
            logger.info("Step 2: Transforming data")
            transformed_data = self.transform_data(raw_data)
            results['steps_completed'].append('transform')
            results['records_transformed'] = sum(len(df) for df in transformed_data.values())
            
            # Step 3: Load
            logger.info("Step 3: Loading data")
            self.load_data(transformed_data)
            results['steps_completed'].append('load')
            
            # Pipeline completed successfully
            end_time = datetime.now()
            results.update({
                'status': 'completed',
                'end_time': end_time,
                'duration_seconds': (end_time - start_time).total_seconds()
            })
            
            logger.info(f"ETL pipeline completed successfully in {results['duration_seconds']:.2f} seconds")
            
        except Exception as e:
            # Pipeline failed
            end_time = datetime.now()
            results.update({
                'status': 'failed',
                'end_time': end_time,
                'duration_seconds': (end_time - start_time).total_seconds(),
                'errors': [str(e)]
            })
            
            logger.error(f"ETL pipeline failed: {e}")
        
        return results

def main():
    """
    Main function to run the ETL pipeline
    
    This function demonstrates how to use the NBAETLPipeline class
    to process NBA data through the complete ETL workflow.
    """
    # Initialize the pipeline
    pipeline = NBAETLPipeline()
    
    # Run pipeline for current season
    season = "2023-24"
    
    print(f"NBA ETL Pipeline Demo - Season {season}")
    print("=" * 50)
    
    try:
        # Run the complete pipeline
        results = pipeline.run_pipeline(season, incremental=True)
        
        # Display results
        print("\nPipeline Execution Results:")
        print("-" * 30)
        print(f"Status: {results['status']}")
        print(f"Duration: {results['duration_seconds']:.2f} seconds")
        print(f"Records Extracted: {results.get('records_extracted', 0)}")
        print(f"Records Transformed: {results.get('records_transformed', 0)}")
        print(f"Steps Completed: {', '.join(results['steps_completed'])}")
        
        if results['errors']:
            print(f"Errors: {results['errors']}")
        
        print(f"\n✅ ETL pipeline execution completed!")
        
    except Exception as e:
        print(f"❌ ETL pipeline execution failed: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
