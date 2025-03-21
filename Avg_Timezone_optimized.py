import aiohttp
import asyncio
from datetime import datetime
import time
from tqdm import tqdm
import pytz
import json
from aiohttp import ClientTimeout
import csv
from collections import defaultdict
import zipcodes
from timezonefinder import TimezoneFinder
import redis
import numpy as np
from typing import Dict, List, Tuple, Optional
import pickle
from statistics import mean, median, stdev
from datetime import datetime, timezone

# Import ZIP codes
from contiguous_usa_zip_codes import ZIP_CODES

# Configuration
API_URL = "https://api.sunrisesunset.io/json"
GRID_SIZE = 1.0  # Size of grid squares in degrees
REDIS_EXPIRY = 86400  # Cache results for 24 hours
BATCH_SIZE = 100  # Number of grids to process in parallel

# Initialize services
tf = TimezoneFinder()
redis_client = redis.Redis(host='localhost', port=6379, db=0)

class GridCache:
    def __init__(self):
        self.redis = redis_client
        
    def get_cache_key(self, lat_grid: float, lon_grid: float) -> str:
        return f"sunset:grid:{lat_grid:.1f}:{lon_grid:.1f}"
        
    def get_cached_result(self, lat_grid: float, lon_grid: float) -> Optional[dict]:
        key = self.get_cache_key(lat_grid, lon_grid)
        result = self.redis.get(key)
        return pickle.loads(result) if result else None
        
    def set_cached_result(self, lat_grid: float, lon_grid: float, data: dict):
        key = self.get_cache_key(lat_grid, lon_grid)
        self.redis.setex(key, REDIS_EXPIRY, pickle.dumps(data))

class GeographicBatcher:
    def __init__(self, zip_codes: List[str]):
        self.zip_codes = zip_codes
        self.grid_cache = GridCache()
        self.grid_map: Dict[Tuple[float, float], List[dict]] = defaultdict(list)
        
    def _is_contiguous_us(self, location: dict) -> bool:
        """Check if a location is in the contiguous United States."""
        state = location.get('state')
        # Exclude Alaska (AK), Hawaii (HI), and Puerto Rico (PR)
        return state not in ['AK', 'HI', 'PR']
        
    def _get_grid_coordinates(self, lat: float, lon: float) -> Tuple[float, float]:
        """Convert exact coordinates to grid coordinates."""
        lat_grid = np.floor(lat / GRID_SIZE) * GRID_SIZE
        lon_grid = np.floor(lon / GRID_SIZE) * GRID_SIZE
        return (lat_grid, lon_grid)
        
    def prepare_batches(self) -> Dict[Tuple[float, float], List[dict]]:
        """Group ZIP codes into geographic grid squares."""
        for zip_code in self.zip_codes:
            location = zipcodes.matching(zip_code)
            if not location:
                continue
                
            location = location[0]
            # Skip if not in contiguous US
            if not self._is_contiguous_us(location):
                continue
                
            lat = float(location['lat'])
            lon = float(location['long'])
            
            # Get timezone information
            timezone_str = tf.timezone_at(lat=lat, lng=lon)
            if not timezone_str:
                continue
                
            tz = pytz.timezone(timezone_str)
            utc_offset = datetime.now(tz).utcoffset().total_seconds() / 3600
            
            grid_coords = self._get_grid_coordinates(lat, lon)
            self.grid_map[grid_coords].append({
                'zip_code': zip_code,
                'lat': lat,
                'lon': lon,
                'timezone_offset': utc_offset
            })
            
        return self.grid_map

class RateLimiter:
    def __init__(self, calls_per_second: int = 10):
        self.calls_per_second = calls_per_second
        self.min_interval = 1.0 / calls_per_second
        self.last_call = 0
        self._lock = asyncio.Lock()

    async def acquire(self):
        async with self._lock:
            now = time.time()
            elapsed = now - self.last_call
            if elapsed < self.min_interval:
                await asyncio.sleep(self.min_interval - elapsed)
            self.last_call = time.time()

async def fetch_grid_sunset(
    session: aiohttp.ClientSession,
    grid_lat: float,
    grid_lon: float,
    locations: List[dict],
    rate_limiter: RateLimiter,
    pbar: tqdm
) -> List[dict]:
    """Fetch sunset time for a grid coordinate and apply to all locations in that grid."""
    try:
        # Check cache first
        cache = GridCache()
        cached_result = cache.get_cached_result(grid_lat, grid_lon)
        
        if cached_result:
            results = []
            for loc in locations:
                results.append({
                    'zip_code': loc['zip_code'],
                    'sunset_time': cached_result['sunset_time'],
                    'timezone_offset': loc['timezone_offset']
                })
            pbar.update(len(locations))
            return results
            
        # If not in cache, fetch from API
        await rate_limiter.acquire()
        timeout = ClientTimeout(total=10)
        
        # Use center of grid for API call
        center_lat = grid_lat + (GRID_SIZE / 2)
        center_lon = grid_lon + (GRID_SIZE / 2)
        
        # Get today's date for the API call
        today = datetime.now().strftime('%Y-%m-%d')
        
        async with session.get(
            f"{API_URL}?lat={center_lat}&lng={center_lon}&date={today}",
            timeout=timeout
        ) as response:
            if response.status != 200:
                print(f"Error: API returned status {response.status} for grid {grid_lat}, {grid_lon}")
                pbar.update(len(locations))
                return []

            data = await response.json()
            if not data or "results" not in data or "sunset" not in data["results"]:
                print(f"Error: Invalid API response for grid {grid_lat}, {grid_lon}")
                pbar.update(len(locations))
                return []

            sunset_time = data["results"]["sunset"]
            
            # Cache the result
            cache.set_cached_result(grid_lat, grid_lon, {
                'sunset_time': sunset_time
            })
            
            # Apply to all locations in grid
            results = []
            for loc in locations:
                results.append({
                    'zip_code': loc['zip_code'],
                    'sunset_time': sunset_time,
                    'timezone_offset': loc['timezone_offset']
                })
            pbar.update(len(locations))
            return results
                
    except Exception as e:
        print(f"Error processing grid {grid_lat}, {grid_lon}: {str(e)}")
        pbar.update(len(locations))
        return []

def time_to_seconds(time_str):
    """Convert time string to seconds since midnight."""
    try:
        # Try parsing 12-hour format (e.g., "7:03:48 PM")
        try:
            time_obj = datetime.strptime(time_str, "%I:%M:%S %p")
            return time_obj.hour * 3600 + time_obj.minute * 60 + time_obj.second
        except ValueError:
            # Try parsing 24-hour format (e.g., "19:03:48")
            try:
                time_obj = datetime.strptime(time_str, "%H:%M:%S")
                return time_obj.hour * 3600 + time_obj.minute * 60 + time_obj.second
            except ValueError:
                # Try parsing ISO format
                if time_str.endswith('Z'):
                    time_str = time_str[:-1] + '+00:00'
                time_obj = datetime.fromisoformat(time_str).time()
                return time_obj.hour * 3600 + time_obj.minute * 60 + time_obj.second
    except Exception as e:
        print(f"Error parsing time '{time_str}': {str(e)}")
        raise e

def seconds_to_time(seconds):
    """Convert seconds since midnight to HH:MM:SS format."""
    hours = (seconds // 3600) % 24  # Ensure hours stay within 0-23 range
    minutes = (seconds % 3600) // 60
    secs = seconds % 60
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"

def calculate_statistics(results: List[dict]) -> dict:
    """Calculate detailed statistics from sunset times."""
    if not results:
        return {}
        
    # Convert all times to seconds for calculations
    seconds_list = [time_to_seconds(r['sunset_time']) for r in results]
    
    # Basic statistics
    avg_seconds = mean(seconds_list)
    median_seconds = median(seconds_list)
    std_seconds = stdev(seconds_list)
    
    # Find earliest and latest with their ZIP codes
    earliest_idx = seconds_list.index(min(seconds_list))
    latest_idx = seconds_list.index(max(seconds_list))
    earliest_data = results[earliest_idx]
    latest_data = results[latest_idx]
    
    # Calculate time ranges
    time_range_seconds = max(seconds_list) - min(seconds_list)
    
    # Group by hour
    hours_distribution = defaultdict(int)
    for seconds in seconds_list:
        hour = seconds // 3600
        hours_distribution[hour] += 1
    
    # Calculate percentiles
    percentiles = {
        "10th": np.percentile(seconds_list, 10),
        "25th": np.percentile(seconds_list, 25),
        "50th": np.percentile(seconds_list, 50),
        "75th": np.percentile(seconds_list, 75),
        "90th": np.percentile(seconds_list, 90)
    }
    
    # Group by timezone offset
    timezone_groups = defaultdict(list)
    for r in results:
        timezone_groups[r['timezone_offset']].append(time_to_seconds(r['sunset_time']))
    
    timezone_stats = {
        str(offset): {
            "count": len(times),
            "average": seconds_to_time(int(mean(times))),
            "std_dev_minutes": f"{(stdev(times) / 60):.2f}" if len(times) > 1 else "N/A"
        }
        for offset, times in timezone_groups.items()
    }
    
    return {
        "summary_statistics": {
            "average_sunset": seconds_to_time(int(avg_seconds)),
            "median_sunset": seconds_to_time(int(median_seconds)),
            "standard_deviation_minutes": f"{(std_seconds / 60):.2f}",
            "total_locations": len(results)
        },
        "range_analysis": {
            "earliest_sunset": {
                "time": earliest_data['sunset_time'],
                "zip_code": earliest_data['zip_code'],
                "timezone_offset": earliest_data['timezone_offset']
            },
            "latest_sunset": {
                "time": latest_data['sunset_time'],
                "zip_code": latest_data['zip_code'],
                "timezone_offset": latest_data['timezone_offset']
            },
            "time_range_minutes": f"{(time_range_seconds / 60):.2f}"
        },
        "percentile_distribution": {
            "10th_percentile": seconds_to_time(int(percentiles["10th"])),
            "25th_percentile": seconds_to_time(int(percentiles["25th"])),
            "median": seconds_to_time(int(percentiles["50th"])),
            "75th_percentile": seconds_to_time(int(percentiles["75th"])),
            "90th_percentile": seconds_to_time(int(percentiles["90th"]))
        },
        "hour_distribution": {
            str(hour): {
                "count": count,
                "percentage": f"{(count/len(results)*100):.1f}%"
            }
            for hour, count in sorted(hours_distribution.items())
        },
        "timezone_analysis": timezone_stats
    }

async def process_all_zips():
    # Initialize batcher and prepare grid batches
    batcher = GeographicBatcher(ZIP_CODES)
    grid_map = batcher.prepare_batches()
    
    total_locations = sum(len(locations) for locations in grid_map.values())
    print(f"Grouped {total_locations} ZIP codes into {len(grid_map)} geographic grids")
    
    rate_limiter = RateLimiter(calls_per_second=10)
    connector = aiohttp.TCPConnector(limit=100)
    
    results = []
    
    # Process grids in batches
    with tqdm(total=total_locations, desc="Processing ZIP codes") as pbar:
        async with aiohttp.ClientSession(connector=connector) as session:
            grid_items = list(grid_map.items())
            
            for i in range(0, len(grid_items), BATCH_SIZE):
                batch = grid_items[i:i + BATCH_SIZE]
                tasks = []
                
                for (grid_lat, grid_lon), locations in batch:
                    task = fetch_grid_sunset(
                        session, grid_lat, grid_lon, locations,
                        rate_limiter, pbar
                    )
                    tasks.append(task)
                
                batch_results = await asyncio.gather(*tasks)
                for grid_results in batch_results:
                    results.extend(grid_results)
    
    # Save results
    with open('sunset_times.csv', 'w', newline='') as csvfile:
        fieldnames = ['zip_code', 'sunset_time', 'timezone_offset']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for result in results:
            writer.writerow(result)
    
    # Calculate and save enhanced summary
    if results:
        # Calculate statistics
        stats = calculate_statistics(results)
        
        summary = {
            'data_summary': {
                'total_processed': len(results),
                'total_zips': len(ZIP_CODES),
                'success_rate': f"{(len(results)/len(ZIP_CODES))*100:.2f}%",
                'unique_grids_processed': len(grid_map)
            },
            'sunset_statistics': stats,
            'processing_info': {
                'grid_size_degrees': GRID_SIZE,
                'cache_expiry_hours': REDIS_EXPIRY / 3600,
                'batch_size': BATCH_SIZE,
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
        }
        
        with open('sunset_summary.json', 'w') as f:
            json.dump(summary, f, indent=4)
        
        print(f"\nProcessed {len(results)} ZIP codes using {len(grid_map)} geographic grids")
        print(f"Average sunset time: {stats['summary_statistics']['average_sunset']}")
        print(f"Time range: {stats['range_analysis']['time_range_minutes']} minutes")
        print(f"Results saved to sunset_times.csv and sunset_summary.json")
    else:
        print("No valid sunset times were collected")

if __name__ == "__main__":
    asyncio.run(process_all_zips()) 