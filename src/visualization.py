import sqlite3
from typing import List, Dict, Any
import pandas as pd
from dataclasses import dataclass
from datetime import datetime
import logging
from pathlib import Path

from analysis import CrimeAnalyzer

class CrimeVisualizer:
    def __init__(self, analyzer: CrimeAnalyzer):
        self.analyzer = analyzer
    
    def create_heatmap(self, year: int, save_path: str):
        """Create crime heatmap using folium"""
        import folium
        from folium.plugins import HeatMap
        
        hotspots = self.analyzer.analyze_crime_hotspots(year)
        
        # Create base map centered on Calgary
        m = folium.Map(location=[51.0447, -114.0719], zoom_start=11)
        
        # Add heatmap layer
        heat_data = [[row['avg_latitude'], row['avg_longitude'], row['total_incidents']] 
                    for _, row in hotspots.iterrows()]
        HeatMap(heat_data).add_to(m)
        
        # Save map
        m.save(save_path)
