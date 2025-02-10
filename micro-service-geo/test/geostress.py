from collections import defaultdict
import asyncio
import websockets
import json
import time
from concurrent.futures import ProcessPoolExecutor

# Configuration for the stress test
WEBSOCKET_ENDPOINT = "ws://172.25.22.158:8001/"
TOKEN = "ylA?9%VHWP46wr4R9cfdF<#m!~j8"
NUMBER_OF_REQUESTS = 20000
NUMBER_OF_PROCESSES = 62

data = json.dumps({"latitude": 45.4869299, "longitude": -73.4690801})
headers = json.dumps({"X-Deliver-Auth": TOKEN})


async def send_websocket_auth_data(websocket):
    try:
        await websocket.send(headers)
    except Exception as e:
        return str(e)


async def send_websocket_data(websocket):
    try:
        await websocket.send(data)
        response = await websocket.recv()
        return response
    except Exception as e:
        return str(e)


async def single_process_stress_test(num_requests):
    response_counter = defaultdict(int)
    start_time = time.time()
    async with websockets.connect(WEBSOCKET_ENDPOINT) as websocket:
        await send_websocket_auth_data(websocket)
        for _ in range(num_requests):
            response = await send_websocket_data(websocket)
            if response:
                response_counter[response] += 1
    end_time = time.time()
    return response_counter, end_time - start_time


def run_single_process_stress_test(num_requests):
    asyncio.set_event_loop(asyncio.new_event_loop())
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(single_process_stress_test(num_requests))


def run_stress_test():
    num_requests_per_process = NUMBER_OF_REQUESTS // NUMBER_OF_PROCESSES
    with ProcessPoolExecutor(max_workers=NUMBER_OF_PROCESSES) as executor:
        futures = [
            executor.submit(run_single_process_stress_test, num_requests_per_process)
            for _ in range(NUMBER_OF_PROCESSES)
        ]
        max_time_taken = 0  # Variable to store the maximum time taken by a process
        total_responses = defaultdict(int)

        for future in futures:
            response_counter, time_taken = future.result()
            max_time_taken = max(
                max_time_taken, time_taken
            )  # Update max_time_taken if current time is greater
            for response, count in response_counter.items():
                total_responses[response] += count

    average_requests_per_second = NUMBER_OF_REQUESTS / max_time_taken
    print(f"Average Requests per Second (Global): {average_requests_per_second:.2f}")
    for response, count in total_responses.items():
        print(f"Response: {response}, Occurrences: {count}")


if __name__ == "__main__":
    run_stress_test()
