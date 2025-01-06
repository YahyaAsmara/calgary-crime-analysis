import sqlite3
from typing import List, Dict, Any
import pandas as pd
from dataclasses import dataclass
from datetime import datetime
import logging
from pathlib import Path

from database import DatabaseManager

class CrimeDataLoader:
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
    
    def load_from_csv(self, filepath: Path):
        """Load crime data from CSV file"""
        try:
            df = pd.read_csv(filepath)
            required_columns = ['Community', 'Category', 'Count', 'Year', 'Month']
            
            if not all(col in df.columns for col in required_columns):
                raise ValueError(f"CSV must contain columns: {required_columns}")
            
            # Process and clean data
            df['date'] = pd.to_datetime(df['date'])
            df['year'] = df['date'].dt.year
            df['month'] = df['date'].dt.month
            
            # Insert data in chunks for better performance
            chunk_size = 1000
            conn = self.db_manager._get_connection()
            
            for i in range(0, len(df), chunk_size):
                chunk = df.iloc[i:i + chunk_size]
                chunk.to_sql('crime_stats', conn, if_exists='append', index=False)
                
            conn.commit()
            conn.close()
            logger.info(f"Successfully loaded {len(df)} records from {filepath}")
            
        except Exception as e:
            logger.error(f"Error loading data: {str(e)}")
            raise