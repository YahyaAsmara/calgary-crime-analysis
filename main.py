import logging
from pathlib import Path
from typing import Optional
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

from database import DatabaseManager
from analysis import CrimeAnalyzer
from data_loader import CrimeDataLoader
from visualization import CrimeVisualizer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('crime_analysis.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class CrimeAnalysisApp:
    def __init__(self, data_path: Optional[Path] = None):
        self.data_path = data_path or Path('data/calgary_crime_stats.csv')
        self.output_path = Path('output')
        self.output_path.mkdir(exist_ok=True)
        
        # Initialize components
        self.db_manager = DatabaseManager()
        self.analyzer = CrimeAnalyzer(self.db_manager)
        self.data_loader = CrimeDataLoader(self.db_manager)
        self.visualizer = CrimeVisualizer(self.analyzer)
    
    def load_data(self):
        """Load data if CSV exists"""
        if self.data_path.exists():
            logger.info(f"Loading data from {self.data_path}")
            self.data_loader.load_from_csv(self.data_path)
        else:
            logger.warning(f"Data file not found at {self.data_path}")

    def run_analysis(self, year: int = None):
        """Run various analyses in parallel"""
        year = year or datetime.now().year
        
        try:
            with ThreadPoolExecutor() as executor:
                # Submit analysis tasks
                hotspot_future = executor.submit(
                    self.analyzer.analyze_crime_hotspots, year)
                trends_future = executor.submit(
                    self.analyzer.calculate_crime_rate_trends, year-3, year)
                
                # Get results
                hotspots = hotspot_future.result()
                trends = trends_future.result()
                
                # Generate outputs
                hotspots.to_csv(self.output_path / 'hotspots_analysis.csv')
                trends.to_csv(self.output_path / 'crime_trends.csv')
                
                # Create visualizations
                self.visualizer.create_heatmap(
                    year, 
                    str(self.output_path / f'crime_heatmap_{year}.html')
                )
                
            logger.info("Analysis completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error in analysis: {str(e)}")
            return False

def parse_args():
    parser = argparse.ArgumentParser(description='Calgary Crime Analysis Tool')
    parser.add_argument('--data', type=str, help='Path to crime data CSV')
    parser.add_argument('--year', type=int, help='Analysis year')
    return parser.parse_args()

def main():
    args = parse_args()
    data_path = Path(args.data) if args.data else None
    
    try:
        app = CrimeAnalysisApp(data_path)
        app.load_data()
        success = app.run_analysis(args.year)
        
        if success:
            logger.info("Application completed successfully")
            return 0
        else:
            logger.error("Application failed")
            return 1
            
    except Exception as e:
        logger.error(f"Fatal error in main: {str(e)}")
        return 1

if __name__ == "__main__":
    exit(main())