import requests
import json
import time

BASE_URL = "http://127.0.0.1:8001"


def stream_records(batch_size, delay=1):
    """
    Continuously fetch records in batches
    """

    while True:
        url = f"{BASE_URL}/record/{batch_size}"

        try:
            response = requests.get(url, timeout=5)
            response.raise_for_status()  # Raise an error for bad status codes
            
            # Parse SSE (Server-Sent Events) format
            lines = response.text.strip().split('\n')
            records = []
            
            for i in range(0, len(lines), 3):  # Each record is 3 lines: event, data, blank
                if i + 1 < len(lines) and lines[i].startswith('event:') and lines[i+1].startswith('data:'):
                    data_line = lines[i+1]
                    # Extract JSON from "data: {...}" format
                    json_str = data_line[6:]  # Remove "data: " prefix
                    try:
                        record = json.loads(json_str)
                        records.append(record)
                    except json.JSONDecodeError:
                        continue
            
            for record in records:
                print(record)  # Print each record as it arrives
                yield record

        except requests.exceptions.ConnectionError:
            print(f"Error: Could not connect to server at {BASE_URL}")
            print("Please make sure the server is running.")
            break
        except requests.exceptions.Timeout:
            print(f"Error: Request to {url} timed out")
            break
        except requests.exceptions.RequestException as e:
            print(f"Error: Failed to fetch records: {e}")
            break

        time.sleep(delay)
