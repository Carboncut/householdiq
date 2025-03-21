import pytest
import asyncio
import aiohttp
import random
import string
import json
from datetime import datetime
from statistics import mean, stdev
from typing import Dict, Tuple, List
import time
import sys
import os

# Add the root directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from services.ingestion.app_ingestion import EventType

# Test configuration
CONCURRENT_REQUESTS = 10
TOTAL_REQUESTS = 50
REQUEST_TIMEOUT = 30
BASE_URL = "http://localhost:8000/v1/ingest"

# Valid test data configuration
VALID_PARTNER_IDS = [1, 2]  # Only partners that exist in the database
VALID_TCF_VERSIONS = [1, 2]  # Only supported TCF versions

# Semaphore for controlling test concurrency
request_semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)

def generate_random_string(length=10):
    """Generate a random string of specified length."""
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=length))

def generate_valid_tcf_string():
    """Generate a valid TCF string for testing."""
    version = random.choice(VALID_TCF_VERSIONS)
    # Create a simple valid TCF string format
    return f"C{version}--{generate_random_string(20)}"

def generate_test_data():
    """Generate random test data."""
    is_child = random.choice([True, False])
    return {
        "partner_id": "test_partner",
        "device_data": "test_device_data",
        "partial_keys": {
            "hashedEmail": "test@example.com",
            "deviceType": random.choice(["mobile", "desktop", "tablet"]),
            "child_flag": is_child,  # Updated field name
            "device_child_flag": is_child,  # Updated field name
            "hashedIP": "127.0.0.1"
        },
        "event_type": "test_event",
        "campaign_id": "test_campaign",
        "consent_flags": {
            "email": True,
            "device": True
        },
        "privacy_signals": {
            "signal1": "value1",
            "signal2": "value2"
        }
    }

async def make_request(session: aiohttp.ClientSession, payload: Dict) -> Tuple[bool, float, str]:
    """Make a single request to the ingestion service."""
    async with request_semaphore:
        start_time = time.time()
        try:
            async with session.post(BASE_URL, json=payload, timeout=REQUEST_TIMEOUT) as response:
                try:
                    result = await response.json()
                    duration = time.time() - start_time
                    if response.status == 200:
                        return True, duration, ""
                    else:
                        error_detail = result.get('detail', str(result))
                        return False, duration, f"HTTP {response.status}: {error_detail}"
                except Exception as e:
                    return False, time.time() - start_time, f"Error parsing response: {str(e)}"
        except asyncio.TimeoutError:
            return False, time.time() - start_time, "Request timed out"
        except Exception as e:
            return False, time.time() - start_time, str(e)

async def run_load_test() -> List[Tuple[bool, float, str]]:
    """Run a load test with concurrent requests."""
    connector = aiohttp.TCPConnector(
        limit=CONCURRENT_REQUESTS,
        ttl_dns_cache=300,
        enable_cleanup_closed=True,
        force_close=True
    )
    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
    
    async with aiohttp.ClientSession(
        connector=connector,
        timeout=timeout,
        raise_for_status=False
    ) as session:
        # Create all tasks at once
        tasks = []
        for _ in range(TOTAL_REQUESTS):
            payload = generate_test_data()
            task = asyncio.create_task(make_request(session, payload))
            tasks.append(task)
        
        # Wait for all tasks to complete
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        processed_results = []
        for result in results:
            if isinstance(result, Exception):
                processed_results.append((False, 0, str(result)))
            else:
                processed_results.append(result)
        
        return processed_results

@pytest.mark.asyncio
async def test_ingestion_service():
    """Test the ingestion service under load."""
    print("\nStarting load test...")
    start_time = time.time()
    
    results = await run_load_test()
    
    total_time = time.time() - start_time
    print(f"\nTest completed in {total_time:.2f} seconds")
    
    # Process results
    successes = [r[0] for r in results]
    durations = [r[1] for r in results if r[0]]
    errors = [r[2] for r in results if not r[0]]
    
    # Calculate metrics
    success_rate = (sum(successes) / len(successes)) * 100
    avg_duration = mean(durations) if durations else 0
    duration_stdev = stdev(durations) if len(durations) > 1 else 0
    
    # Log detailed results
    print(f"\nTest Results:")
    print(f"Total Requests: {len(results)}")
    print(f"Successful Requests: {sum(successes)}")
    print(f"Failed Requests: {len(errors)}")
    print(f"Success Rate: {success_rate:.2f}%")
    print(f"Average Duration: {avg_duration:.2f}s")
    print(f"Duration StdDev: {duration_stdev:.2f}s")
    print(f"Requests per Second: {len(results)/total_time:.2f}")
    
    if errors:
        print("\nError Breakdown:")
        error_types = {}
        for error in errors:
            error_types[error] = error_types.get(error, 0) + 1
        for error, count in error_types.items():
            print(f"{error}: {count} occurrences")
    
    # Assertions
    assert success_rate >= 80, f"Success rate should be above 80% (current: {success_rate:.2f}%)"
    assert avg_duration < 2.0, f"Average duration should be under 2s (current: {avg_duration:.2f}s)"
    assert duration_stdev < 1.0, f"Duration standard deviation should be under 1s (current: {duration_stdev:.2f}s)"

@pytest.mark.asyncio
async def test_ingestion_validation():
    """Test input validation of the ingestion service."""
    connector = aiohttp.TCPConnector(limit=1)
    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
    
    async with aiohttp.ClientSession(
        connector=connector,
        timeout=timeout,
        raise_for_status=False
    ) as session:
        # Test invalid partner_id
        invalid_payload = generate_test_data()
        invalid_payload["partner_id"] = -1
        async with session.post(BASE_URL, json=invalid_payload) as response:
            assert response.status in [400, 422], f"Should reject invalid partner_id, got {response.status}"
            result = await response.json()
            print(f"Invalid partner_id response: {result}")
        
        # Test missing required fields
        invalid_payload = {"partner_id": 1}  # Missing required fields
        async with session.post(BASE_URL, json=invalid_payload) as response:
            assert response.status == 422, f"Should reject missing required fields, got {response.status}"
            result = await response.json()
            print(f"Missing fields response: {result}")
        
        # Test invalid event_type
        invalid_payload = generate_test_data()
        invalid_payload["event_type"] = "invalid_type"
        headers = {"Content-Type": "application/json"}
        async with session.post(BASE_URL, json=invalid_payload, headers=headers) as response:
            assert response.status == 422, f"Should reject invalid event_type, got {response.status}"
            result = await response.json()
            print(f"Invalid event_type response: {result}")

@pytest.mark.asyncio
async def test_privacy_handling():
    """Test privacy signal handling."""
    connector = aiohttp.TCPConnector(limit=1)
    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
    
    async with aiohttp.ClientSession(
        connector=connector,
        timeout=timeout,
        raise_for_status=False
    ) as session:
        # Test with no privacy signals
        payload = generate_test_data()
        payload["privacy_signals"] = None
        async with session.post(BASE_URL, json=payload) as response:
            assert response.status == 200, f"Should handle missing privacy signals, got {response.status}"
            result = await response.json()
            print(f"No privacy signals response: {result}")
        
        # Test with invalid TCF string format
        payload = generate_test_data()
        payload["privacy_signals"]["tcf_string"] = "invalid_format"
        async with session.post(BASE_URL, json=payload) as response:
            assert response.status == 200, f"Should handle invalid TCF string format, got {response.status}"
            result = await response.json()
            print(f"Invalid TCF format response: {result}")
            assert "error" not in result, "Should not return error for invalid TCF format"
        
        # Test with unsupported TCF version
        payload = generate_test_data()
        payload["privacy_signals"]["tcf_string"] = "C99--" + generate_random_string(20)
        async with session.post(BASE_URL, json=payload) as response:
            assert response.status == 200, f"Should handle unsupported TCF version, got {response.status}"
            result = await response.json()
            print(f"Unsupported TCF version response: {result}")
            assert "error" not in result, "Should not return error for unsupported TCF version"
        
        # Test with invalid US privacy string
        payload = generate_test_data()
        payload["privacy_signals"]["us_privacy_string"] = "invalid"
        async with session.post(BASE_URL, json=payload) as response:
            assert response.status == 200, f"Should handle invalid US privacy string, got {response.status}"
            result = await response.json()
            print(f"Invalid US privacy string response: {result}")
            assert "error" not in result, "Should not return error for invalid US privacy string"

@pytest.mark.asyncio
async def test_consent_handling():
    """Test consent flag handling."""
    connector = aiohttp.TCPConnector(limit=1)
    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
    
    async with aiohttp.ClientSession(
        connector=connector,
        timeout=timeout,
        raise_for_status=False
    ) as session:
        # Test with no consent
        payload = generate_test_data()
        payload["consent_flags"]["cross_device_bridging"] = False
        async with session.post(BASE_URL, json=payload) as response:
            assert response.status == 200, f"Should handle no consent, got {response.status}"
            result = await response.json()
            print(f"No consent response: {result}")
            assert result.get("bridging_skip_reason") == "NO_CONSENT_OR_FLAGS", "Should indicate no consent"
            assert result.get("bridging_token") is None, "Should not have bridging token"
        
        # Test with child flag
        payload = generate_test_data()
        payload["partial_keys"]["isChild"] = True
        payload["partial_keys"]["deviceChildFlag"] = True
        async with session.post(BASE_URL, json=payload) as response:
            assert response.status == 200, f"Should handle child flag, got {response.status}"
            result = await response.json()
            print(f"Child flag response: {result}")
            assert result.get("bridging_skip_reason") == "CHILD_FLAG", "Should indicate child flag"
            assert result.get("bridging_token") is None, "Should not have bridging token"

if __name__ == "__main__":
    asyncio.run(test_ingestion_service())
    asyncio.run(test_ingestion_validation())
    asyncio.run(test_privacy_handling())
    asyncio.run(test_consent_handling()) 