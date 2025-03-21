# Time Zone Map

A Python application that calculates average sunset times across US ZIP codes using the Sunrise-Sunset API.

## Features

- Asynchronous processing of ZIP codes
- Rate limiting to respect API constraints
- Geographic coordinate conversion
- Timezone offset calculation
- CSV output of sunset times
- JSON summary statistics

## Requirements

- Python 3.7+
- Required packages (install via pip):
  - aiohttp
  - pytz
  - tqdm
  - zipcodes
  - timezonefinder

## Installation

1. Clone the repository
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

Run the main script:
```bash
python Avg_Timezone_fixed.py
```

The script will:
1. Process all ZIP codes in the contiguous USA
2. Calculate sunset times for each location
3. Generate a CSV file with detailed results
4. Create a JSON summary with average sunset time

## Output Files

- `sunset_times.csv`: Detailed sunset times per ZIP code
- `sunset_summary.json`: Summary statistics including average sunset time

## License

MIT License 