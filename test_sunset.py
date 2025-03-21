import unittest
import json
import csv
import os
import asyncio
import aiohttp
from datetime import datetime, timedelta
import redis
from Avg_Timezone_optimized import (
    GeographicBatcher,
    GridCache,
    time_to_seconds,
    seconds_to_time,
    calculate_statistics,
    ZIP_CODES
)

class TestSunsetCalculator(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Initialize test environment."""
        cls.redis_client = redis.Redis(host='localhost', port=6379, db=0)
        # Use first 10 ZIP codes for quick testing
        cls.test_zip_codes = ZIP_CODES[:10]
        
    @classmethod
    def tearDownClass(cls):
        """Clean up after tests."""
        # Clear test data from Redis
        for key in cls.redis_client.keys("sunset:grid:*"):
            cls.redis_client.delete(key)
            
    def test_geographic_batching(self):
        """Test that ZIP codes are correctly grouped into grids."""
        batcher = GeographicBatcher(self.test_zip_codes)
        grid_map = batcher.prepare_batches()
        
        self.assertTrue(len(grid_map) > 0, "Should create at least one grid")
        
        # Verify grid coordinates
        for (lat, lon), locations in grid_map.items():
            self.assertTrue(-90 <= lat <= 90, f"Invalid latitude: {lat}")
            self.assertTrue(-180 <= lon <= 180, f"Invalid longitude: {lon}")
            self.assertTrue(len(locations) > 0, "Grid should contain locations")
            
    def test_redis_caching(self):
        """Test Redis caching functionality."""
        cache = GridCache()
        test_data = {"sunset_time": "7:30:00 PM"}
        
        # Test setting cache
        cache.set_cached_result(35.0, -75.0, test_data)
        
        # Test getting cache
        cached = cache.get_cached_result(35.0, -75.0)
        self.assertEqual(cached, test_data, "Cache retrieval failed")
        
    def test_time_conversion(self):
        """Test time conversion functions."""
        test_times = [
            "7:30:00 PM",
            "19:30:00",
            "2023-05-01T19:30:00Z"
        ]
        
        for time_str in test_times:
            seconds = time_to_seconds(time_str)
            self.assertTrue(0 <= seconds < 86400, f"Invalid seconds for {time_str}")
            
            # Test conversion back to time
            time_str_converted = seconds_to_time(seconds)
            self.assertTrue(":" in time_str_converted, f"Invalid time format: {time_str_converted}")
            
    def test_statistics_calculation(self):
        """Test statistics calculation."""
        test_results = [
            {"zip_code": "12345", "sunset_time": "7:00:00 PM", "timezone_offset": -4},
            {"zip_code": "23456", "sunset_time": "7:30:00 PM", "timezone_offset": -4},
            {"zip_code": "34567", "sunset_time": "8:00:00 PM", "timezone_offset": -5}
        ]
        
        stats = calculate_statistics(test_results)
        
        # Verify required fields
        self.assertIn("summary_statistics", stats)
        self.assertIn("range_analysis", stats)
        self.assertIn("percentile_distribution", stats)
        self.assertIn("hour_distribution", stats)
        self.assertIn("timezone_analysis", stats)

class AsyncTestSunsetCalculator(unittest.TestCase):
    async def test_api_connection(self):
        """Test connection to Sunset-Sunrise API."""
        async with aiohttp.ClientSession() as session:
            async with session.get("https://api.sunrisesunset.io/json?lat=40.7128&lng=-74.0060") as response:
                self.assertEqual(response.status, 200, "API connection failed")
                data = await response.json()
                self.assertIn("results", data, "Invalid API response format")
                self.assertIn("sunset", data["results"], "No sunset time in response")

class OutputTestSunsetCalculator(unittest.TestCase):
    def test_output_files_small_subset(self):
        """Test that output files are created with correct format using a small subset of data."""
        # Temporarily modify ZIP_CODES for quick testing
        original_zip_codes = ZIP_CODES.copy()
        ZIP_CODES.clear()
        ZIP_CODES.extend(original_zip_codes[:10])  # Use only first 10 ZIP codes
        
        try:
            # Run the main script
            os.system("python Avg_Timezone_optimized.py")
            
            # Check CSV file
            self.assertTrue(os.path.exists("sunset_times.csv"), "CSV file not created")
            with open("sunset_times.csv", 'r') as f:
                reader = csv.DictReader(f)
                row = next(reader)
                self.assertIn("zip_code", row)
                self.assertIn("sunset_time", row)
                self.assertIn("timezone_offset", row)
                
            # Check JSON file
            self.assertTrue(os.path.exists("sunset_summary.json"), "JSON file not created")
            with open("sunset_summary.json", 'r') as f:
                data = json.load(f)
                self.assertIn("data_summary", data)
                self.assertIn("sunset_statistics", data)
                self.assertIn("processing_info", data)
        finally:
            # Restore original ZIP_CODES
            ZIP_CODES.clear()
            ZIP_CODES.extend(original_zip_codes)

async def run_async_tests():
    """Run async tests."""
    async_suite = unittest.TestLoader().loadTestsFromTestCase(AsyncTestSunsetCalculator)
    for test in async_suite._tests:
        await test.test_api_connection()
    print("Async tests completed successfully!")

if __name__ == "__main__":
    # Run synchronous tests
    print("\nRunning synchronous tests...")
    sync_suite = unittest.TestLoader().loadTestsFromTestCase(TestSunsetCalculator)
    unittest.TextTestRunner(verbosity=2).run(sync_suite)
    
    # Run output tests with small subset
    print("\nRunning output tests with small subset...")
    output_suite = unittest.TestLoader().loadTestsFromTestCase(OutputTestSunsetCalculator)
    unittest.TextTestRunner(verbosity=2).run(output_suite)
    
    # Run async tests
    print("\nRunning asynchronous tests...")
    asyncio.run(run_async_tests()) 