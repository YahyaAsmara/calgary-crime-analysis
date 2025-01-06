# Calgary Crime Statistics Analysis Platform

A sophisticated data analysis platform for processing, visualizing, and analyzing Calgary crime statistics using advanced SQL queries and machine learning techniques.

## Technical Architecture

- **Backend**: Python 3.10+ with SQLite
- **Spatial Analysis**: Folium with heatmap generation
- **Data Processing**: Pandas with chunked processing
- **Visualization**: Interactive maps and Matplotlib/Seaborn charts
- **Database**: SQLite with spatial indexing

## Core Features

### Data Processing
- Automated CSV data ingestion with validation
- Chunked processing for large datasets
- Spatial data normalization
- Population-weighted analysis

### Analysis Capabilities
- Crime hotspot detection
- Temporal trend analysis
- Population-adjusted crime rates
- Year-over-year change calculation
- Spatial clustering

### Visualization
- Interactive heatmaps
- Temporal trend charts
- Community-based comparisons
- Export capabilities

## Installation

1. Environment Setup:
```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows
```

2. Install Dependencies:
```bash
pip install -r requirements.txt
```

3. Program Initialization:
```bash
python main.py
```

## Usage

### Data Import
```python
from data_loader import CrimeDataLoader
loader = CrimeDataLoader()
loader.load_from_csv('path/to/data.csv')
```

### Analysis
```python
from analysis import CrimeAnalyzer
analyzer = CrimeAnalyzer()
hotspots = analyzer.analyze_crime_hotspots(YEAR)
```

### Visualization
```python
from visualization import CrimeVisualizer
visualizer = CrimeVisualizer()
visualizer.create_heatmap(YEAR, 'output/heatmap.html')
```

## Project Structure
```
calgary-crime-analysis/
├── src/
│   ├── database.py
│   ├── analysis.py
│   ├── visualization.py
│   └── data_loader.py
|    main.py
├── data/
│   └── .gitkeep
└── output/
    └── .gitkeep
```

## Error Handling

The platform includes comprehensive error handling for:
- Data validation errors
- Database connection issues
- Spatial analysis errors
- Memory management
- File I/O operations

## Contributing

1. Fork repository
2. Create feature branch
3. Follow PEP 8 standards
4. Include unit tests
5. Submit pull request

## Data

https://data.calgarypolice.ca/
https://open.alberta.ca/dataset?res_format=CSV&organization=justiceandsolicitorgeneral
https://data.calgary.ca/Health-and-Safety/Community-Crime-Statistics/78gh-n26t/about_data

## License

MIT License
