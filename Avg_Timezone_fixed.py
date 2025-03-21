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
from datetime import datetime, timezone
import pytz

# List of ZIP codes (expand this list)
from contiguous_usa_zip_codes import ZIP_CODES

# API URL (Replace with your actual API URL)
API_URL = "https://api.sunrisesunset.io/json"

# Initialize TimezoneFinder (this is thread-safe)
tf = TimezoneFinder()

class RateLimiter:
    def __init__(self, calls_per_second):
        self.calls_per_second = calls_per_second
        self.min_interval = 1.0 / calls_per_second
        self.last_call = 0
        self._lock = asyncio.Lock()
        self._bucket = defaultdict(float)

    async def acquire(self):
        async with self._lock:
            now = time.time()
            elapsed = now - self.last_call
            if elapsed < self.min_interval:
                await asyncio.sleep(self.min_interval - elapsed)
            self.last_call = time.time()

def zip_to_latlon(zip_code):
    """Convert ZIP code to latitude, longitude, and timezone offset."""
    try:
        # Get ZIP code data from the zipcodes library
        location = zipcodes.matching(zip_code)
        if not location:
            return None
        
        location = location[0]  # Get first match
        lat = float(location['lat'])
        lon = float(location['long'])
        
        # Find the timezone for this location
        timezone_str = tf.timezone_at(lat=lat, lng=lon)
        if not timezone_str:
            return None
            
        # Get the timezone object
        tz = pytz.timezone(timezone_str)
        
        # Get current UTC offset (considering DST)
        current_time = datetime.now(tz)
        utc_offset = current_time.utcoffset().total_seconds() / 3600
        
        return {
            "latitude": lat,
            "longitude": lon,
            "utc_offset": utc_offset
        }
    except Exception as e:
        print(f"Error processing ZIP code {zip_code}: {e}")
        return None

# Function to fetch sunset time for a given ZIP code
async def get_sunset_time(session, zip_code, rate_limiter, pbar):
    try:
        await rate_limiter.acquire()
        # Get latitude and longitude for ZIP code
        geo_data = zip_to_latlon(zip_code)
        if not geo_data:
            print(f"Could not find location data for ZIP code {zip_code}")
            pbar.update(1)
            return None
            
        lat, lon, timezone_offset = geo_data["latitude"], geo_data["longitude"], geo_data["utc_offset"]
        
        timeout = ClientTimeout(total=10)  # 10 second timeout
        async with session.get(f"{API_URL}?lat={lat}&lng={lon}&formatted=0", timeout=timeout) as response:
            data = await response.json()
            
            if response.status == 200 and "results" in data:
                sunset_time = data["results"]["sunset"]
                seconds = time_to_seconds(sunset_time)
                pbar.update(1)
                if seconds is not None:
                    return {
                        'zip_code': zip_code,
                        'sunset_time': sunset_time,
                        'seconds': seconds,
                        'timezone_offset': timezone_offset
                    }
            else:
                print(f"Error fetching sunset for ZIP {zip_code}: {data}")
    except Exception as e:
        print(f"Error processing {zip_code}: {e}")
    pbar.update(1)
    return None

def convert_12h_to_iso(time_str):
    """Convert 12-hour time format to ISO format."""
    try:
        # Parse the 12-hour time format
        parsed_time = datetime.strptime(time_str, "%I:%M:%S %p")
        # Convert to ISO format with current date and UTC timezone
        current_date = datetime.now().date()
        dt = datetime.combine(current_date, parsed_time.time())
        return dt.replace(tzinfo=timezone.utc).isoformat()
    except ValueError as e:
        print(f"Error converting time format {time_str}: {e}")
        return None

# Convert sunset time (ISO format) to seconds since midnight
def time_to_seconds(time_str):
    try:
        # First try to convert from 12-hour format if needed
        if ' PM' in time_str or ' AM' in time_str:
            time_str = convert_12h_to_iso(time_str)
            if time_str is None:
                return None
        
        # Parse ISO format time
        sunset_dt = datetime.fromisoformat(time_str)
        # Convert to seconds since midnight
        return sunset_dt.hour * 3600 + sunset_dt.minute * 60 + sunset_dt.second
    except ValueError as e:
        print(f"Error parsing time {time_str}: {e}")
        return None

# Convert seconds back to HH:MM:SS
def seconds_to_time(seconds):
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"

async def process_all_zips():
    rate_limiter = RateLimiter(calls_per_second=10)
    connector = aiohttp.TCPConnector(limit=100)
    
    # Create CSV file and writer at the start
    with open('sunset_times.csv', 'w', newline='') as csvfile:
        fieldnames = ['zip_code', 'sunset_time', 'timezone_offset']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        # Create chunks of ZIP codes for batch processing
        chunk_size = 500
        zip_chunks = [ZIP_CODES[i:i + chunk_size] for i in range(0, len(ZIP_CODES), chunk_size)]
        all_results = []
        
        try:
            async with aiohttp.ClientSession(connector=connector) as session:
                for chunk_index, chunk in enumerate(zip_chunks):
                    print(f"\nProcessing chunk {chunk_index + 1}/{len(zip_chunks)}")
                    with tqdm(total=len(chunk), desc=f"Processing {len(chunk)} ZIP codes") as pbar:
                        tasks = [
                            get_sunset_time(session, zip_code, rate_limiter, pbar)
                            for zip_code in chunk
                        ]
                        chunk_results = await asyncio.gather(*tasks)
                        valid_results = [r for r in chunk_results if r is not None]
                        
                        # Write chunk results to CSV immediately
                        for result in valid_results:
                            try:
                                writer.writerow({
                                    'zip_code': result['zip_code'],
                                    'sunset_time': result['sunset_time'],
                                    'timezone_offset': result['timezone_offset']
                                })
                                csvfile.flush()  # Force write to disk
                                print(f"Wrote data for ZIP: {result['zip_code']}")  # Debug logging
                            except Exception as e:
                                print(f"Error writing to CSV for ZIP {result['zip_code']}: {e}")
                        
                        all_results.extend(valid_results)
                        print(f"Chunk {chunk_index + 1} complete. Valid results in chunk: {len(valid_results)}")
            
            # Calculate and save average
            if all_results:
                valid_times = [r['seconds'] for r in all_results]
                avg_seconds = sum(valid_times) / len(valid_times)
                avg_time = seconds_to_time(int(avg_seconds))
                
                summary = {
                    'average_sunset': avg_time,
                    'total_processed': len(all_results),
                    'total_zips': len(ZIP_CODES),
                    'success_rate': f"{(len(all_results)/len(ZIP_CODES))*100:.2f}%"
                }
                
                # Save summary to JSON
                with open('sunset_summary.json', 'w') as f:
                    json.dump(summary, f, indent=4)
                    
                print(f"\nAverage sunset time: {avg_time}")
                print(f"Processed {len(all_results)} valid sunset times")
                print(f"Results saved to sunset_times.csv and sunset_summary.json")
            else:
                print("No valid sunset times were collected")
                
        except Exception as e:
            print(f"Error during processing: {e}")
            raise  # Re-raise the exception for debugging

if __name__ == "__main__":
    asyncio.run(process_all_zips()) 