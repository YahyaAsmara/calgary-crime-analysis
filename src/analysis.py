import sqlite3
from typing import List, Dict, Any
import pandas as pd
from dataclasses import dataclass
from datetime import datetime
import logging
from pathlib import Path

from database import DatabaseManager

class CrimeAnalyzer:
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
    
    def analyze_crime_hotspots(self, year: int, min_incidents: int = 5) -> pd.DataFrame:
        """Analyze crime hotspots using spatial clustering"""
        query = '''
        WITH monthly_counts AS (
            SELECT 
                community,
                longitude,
                latitude,
                strftime('%m', date) as month,
                SUM(count) as incident_count
            FROM crime_stats
            WHERE year = ?
            GROUP BY community, month
            HAVING incident_count >= ?
        )
        SELECT 
            community,
            AVG(longitude) as avg_longitude,
            AVG(latitude) as avg_latitude,
            COUNT(DISTINCT month) as active_months,
            SUM(incident_count) as total_incidents
        FROM monthly_counts
        GROUP BY community
        ORDER BY total_incidents DESC
        '''
        
        with self.db_manager._get_connection() as conn:
            return pd.read_sql_query(query, conn, params=(year, min_incidents))
    
    def calculate_crime_rate_trends(self, start_year: int, end_year: int) -> pd.DataFrame:
        """Calculate crime rates per 100,000 population with trend analysis"""
        query = '''
        WITH yearly_stats AS (
            SELECT 
                cs.year,
                cs.community,
                SUM(cs.count) as incidents,
                MAX(cst.population) as population
            FROM crime_stats cs
            LEFT JOIN community_stats cst ON cs.community = cst.community
            WHERE cs.year BETWEEN ? AND ?
            GROUP BY cs.year, cs.community
        )
        SELECT 
            year,
            community,
            incidents,
            population,
            ROUND(incidents * 100000.0 / population, 2) as crime_rate,
            ROUND((incidents * 100000.0 / population) - 
                LAG(incidents * 100000.0 / population) OVER (
                    PARTITION BY community ORDER BY year
                ), 2) as year_over_year_change
        FROM yearly_stats
        WHERE population > 0
        ORDER BY year DESC, crime_rate DESC
        '''
        
        with self.db_manager._get_connection() as conn:
            return pd.read_sql_query(query, conn, params=(start_year, end_year))
class CrimeAnalyzer:
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
    
    def analyze_crime_hotspots(self, year: int, min_incidents: int = 5) -> pd.DataFrame:
        """Analyze crime hotspots using spatial clustering"""
        query = '''
        WITH monthly_counts AS (
            SELECT 
                community,
                longitude,
                latitude,
                strftime('%m', date) as month,
                SUM(count) as incident_count
            FROM crime_stats
            WHERE year = ?
            GROUP BY community, month
            HAVING incident_count >= ?
        )
        SELECT 
            community,
            AVG(longitude) as avg_longitude,
            AVG(latitude) as avg_latitude,
            COUNT(DISTINCT month) as active_months,
            SUM(incident_count) as total_incidents
        FROM monthly_counts
        GROUP BY community
        ORDER BY total_incidents DESC
        '''
        
        with self.db_manager._get_connection() as conn:
            return pd.read_sql_query(query, conn, params=(year, min_incidents))
    
    def calculate_crime_rate_trends(self, start_year: int, end_year: int) -> pd.DataFrame:
        """Calculate crime rates per 100,000 population with trend analysis"""
        query = '''
        WITH yearly_stats AS (
            SELECT 
                cs.year,
                cs.community,
                SUM(cs.count) as incidents,
                MAX(cst.population) as population
            FROM crime_stats cs
            LEFT JOIN community_stats cst ON cs.community = cst.community
            WHERE cs.year BETWEEN ? AND ?
            GROUP BY cs.year, cs.community
        )
        SELECT 
            year,
            community,
            incidents,
            population,
            ROUND(incidents * 100000.0 / population, 2) as crime_rate,
            ROUND((incidents * 100000.0 / population) - 
                LAG(incidents * 100000.0 / population) OVER (
                    PARTITION BY community ORDER BY year
                ), 2) as year_over_year_change
        FROM yearly_stats
        WHERE population > 0
        ORDER BY year DESC, crime_rate DESC
        '''
        
        with self.db_manager._get_connection() as conn:
            return pd.read_sql_query(query, conn, params=(start_year, end_year))