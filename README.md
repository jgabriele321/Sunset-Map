# Time Zone Map

A high-performance Python application that calculates and analyzes sunset times across US ZIP codes using the Sunrise-Sunset API. The application employs geographic batching, Redis caching, and parallel processing to efficiently process tens of thousands of locations.

## Features

- **Geographic Batching**: Groups ZIP codes into 1-degree grid squares to minimize API calls
- **Redis Caching**: Caches sunset times for 24 hours to improve subsequent runs
- **Parallel Processing**: Processes multiple geographic grids concurrently
- **Comprehensive Statistics**:
  - Average and median sunset times
  - Standard deviation and percentile distribution
  - Earliest and latest sunset locations
  - Hour-by-hour distribution
  - Timezone-specific analysis
- **Rate Limiting**: Respects API constraints while maximizing throughput
- **Progress Tracking**: Real-time progress bar during processing

## Requirements

- Python 3.7+
- Redis server
- Required Python packages (install via pip):
  - aiohttp: Asynchronous HTTP client
  - pytz: Timezone handling
  - tqdm: Progress bar
  - zipcodes: ZIP code database
  - timezonefinder: Timezone lookup
  - redis: Redis client
  - numpy: Numerical computations

## Installation

1. Clone the repository
2. Install Redis:
   ```bash
   # macOS (using Homebrew)
   brew install redis
   brew services start redis
   
   # Linux
   sudo apt-get install redis-server
   sudo systemctl start redis-server
   ```
3. Install Python dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

Run the optimized version:
```bash
python Avg_Timezone_optimized.py
```

The script will:
1. Group ZIP codes into geographic grids
2. Process locations in parallel with caching
3. Generate detailed statistics
4. Save results to output files

## Output Files

### sunset_times.csv
Detailed CSV file containing:
- ZIP code
- Sunset time
- Timezone offset

### sunset_summary.json
Comprehensive JSON summary including:
- Data processing statistics
- Average sunset time
- Time range analysis
- Percentile distribution
- Hour-by-hour distribution
- Timezone-specific analysis
- Processing metadata

## Performance

- Reduces API calls by ~97% through geographic batching
- Processes 40,000+ ZIP codes in minutes
- Caches results for faster subsequent runs
- Handles rate limiting automatically

## Configuration

Key parameters in `Avg_Timezone_optimized.py`:
- `GRID_SIZE`: Size of geographic grid squares (default: 1.0 degrees)
- `REDIS_EXPIRY`: Cache duration (default: 24 hours)
- `BATCH_SIZE`: Parallel processing batch size (default: 100)

## License

MIT License 