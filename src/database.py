import sqlite3
from typing import List, Dict, Any
import pandas as pd
from dataclasses import dataclass
from datetime import datetime
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class CrimeRecord:
    sector: str
    community: str
    category: str
    date: datetime
    count: int
    year: int
    month: int
    longitude: float
    latitude: float

class DatabaseManager:
    def __init__(self, db_path: str = 'calgary_crime.db'):
        self.db_path = db_path
        self._init_database()
    
    def _init_database(self):
        """Initialize database with enhanced schema"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS crime_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sector VARCHAR(50),
            community VARCHAR(100),
            category VARCHAR(100),
            date DATE,
            count INTEGER,
            year INTEGER,
            month INTEGER,
            longitude FLOAT,
            latitude FLOAT,
            UNIQUE(sector, community, category, date)
        )''')
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS community_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            community VARCHAR(100) UNIQUE,
            population INTEGER,
            median_income FLOAT,
            area_sqkm FLOAT
        )''')
        
        # Create indices for better query performance
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_date ON crime_stats(date)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_community ON crime_stats(community)')
        
        conn.commit()
        conn.close()

    def _get_connection(self):
        return sqlite3.connect(self.db_path)