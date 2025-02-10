from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from geodatabase import get_db, MICRO_SERVICE_KEY
from shapely.geometry import Point, LineString
from sqlalchemy.exc import SQLAlchemyError
from geopy.distance import geodesic
from datetime import datetime
from sqlalchemy import text
import polyline
import binascii
import aiohttp
import logging
import orjson
import gzip
import json
import io

app = FastAPI(
    debug=False,
    docs_url=None,
    redoc_url=None,
)


# Number of data points to collect in each row for history and time for updating the driver's current location
DATA_UPDATE_THRESHOLD = 8

# Number of bundles to collect before performing a database history
BUNDLE_THRESHOLD = 10

# Number of data points to collect before performing a database history
DATA_POINTS_THRESHOLD = DATA_UPDATE_THRESHOLD * BUNDLE_THRESHOLD

# Time between each polyline fetch
TIME_BETWEEN_POLYLINE_FETCH = 30

# Polyline fetch limit
POLYLINE_FETCH_LIMIT = 15


async def authenticate_ws(
    token: str,
) -> int:
    if not token:
        logging.warning("Authentication attempt with empty token")
        return None

    try:
        async for db in get_db():
            result = await db.execute(
                text(
                    "SELECT id FROM drivers WHERE token = :token AND token_expiration > CURRENT_TIMESTAMP LIMIT 1"
                ),
                {"token": token},
            )
            return result.scalar_one_or_none()
    except SQLAlchemyError as e:
        logging.error(f"Database error during authentication: {e}")
        raise e
    except Exception as e:
        logging.error(f"Unexpected error during authentication: {e}")
        raise e


async def authenticate_websocket(
    websocket: WebSocket,
    token: str,
) -> str | None:
    if not websocket or not token:
        logging.warning("Missing websocket or token for authentication")
        return None

    try:
        driver_id = await authenticate_ws(token)
        if not driver_id:
            await websocket.close()
            return None
        return driver_id
    except WebSocketDisconnect:
        logging.error("WebSocket disconnected during authentication")
        return None
    except Exception as e:
        logging.error(f"Unexpected error during WebSocket authentication: {e}")
        raise e


async def get_polyline_with_instruction(
    driver_id: int,
) -> str | None:
    # Input validation
    if not isinstance(driver_id, int) or driver_id <= 0:
        logging.error("Invalid driver_id provided")
        return None

    try:
        async for db in get_db():
            result = await db.execute(
                text(
                    """
                    SELECT polyline_with_instruction 
                    FROM routes 
                    WHERE driver_id = :driver_id;
                    """
                ),
                {"driver_id": driver_id},
            )
            compressed_polyline = result.scalar_one_or_none()

            if compressed_polyline is None:
                return None

            # Decompress the polyline data
            try:
                polyline_with_instruction = gzip.decompress(compressed_polyline).decode(
                    "utf-8"
                )
                return polyline_with_instruction
            except gzip.BadGzipFile:
                logging.error(f"Invalid gzip data for driver_id {driver_id}")
                return None
            except UnicodeDecodeError:
                logging.error(
                    f"Decoded data is not valid UTF-8 for driver_id {driver_id}"
                )
                return None
    except SQLAlchemyError as e:
        logging.error(f"Database error while fetching polyline_with_instruction: {e}")
        raise e
    except Exception as e:
        logging.error(f"Unexpected error in get_polyline_with_instruction: {e}")
        raise e


async def update_current_location(
    driver_id: str,
    latitude: float,
    longitude: float,
) -> None:
    if not driver_id:
        logging.error("Driver ID is required.")
        raise ValueError("Driver ID is required.")

    if not -90 <= latitude <= 90 or not -180 <= longitude <= 180:
        logging.error("Invalid latitude or longitude values.")
        raise ValueError("Invalid latitude or longitude values.")

    try:
        async for db in get_db():
            result = await db.execute(
                text(
                    """
                    UPDATE routes 
                    SET driver_latitude = :latitude, driver_longitude = :longitude
                    WHERE driver_id = :driver_id;
                    """
                ),
                {
                    "driver_id": driver_id,
                    "latitude": latitude,
                    "longitude": longitude,
                },
            )
            if result.rowcount == 0:
                logging.warning(f"No record updated for driver_id {driver_id}")
            await db.commit()
    except SQLAlchemyError as e:
        await db.rollback()
        logging.error(f"Database operation failed: {e}")
        raise e
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        raise e


async def bundle_data_points(
    data_points: list[tuple[float, float, datetime]],
    batch_start_time: datetime,
    now: datetime,
) -> dict:
    if not data_points:
        logging.warning("No data points provided.")
        return {}

    if any(not isinstance(point, tuple) or len(point) != 3 for point in data_points):
        logging.error("Data points format is invalid.")
        raise ValueError("Invalid format for data points.")

    try:
        finish_time = now
        average_speed_kmh = calculate_average_speed_with_timestamps(
            [point[:2] for point in data_points],
            batch_start_time,
            finish_time,
        )
        return {
            "data_points": data_points,
            "average_speed_kmh": average_speed_kmh,
            "start_time": batch_start_time,
            "finish_time": finish_time,
        }
    except Exception as e:
        logging.error(f"Error in bundling data points: {e}")
        raise e


async def insert_driving_history(
    driver_id: str,
    bundle_data: list[dict],
    now: datetime,
) -> None:
    if not bundle_data:
        logging.warning("Empty bundle data provided")
        return
    if not driver_id:
        logging.warning("No driver ID provided")
        return
    if not now:
        logging.warning("No timestamp provided")
        return

    try:
        async for db in get_db():
            driving_data_json = orjson.dumps(bundle_data)
            compressed_driving_data = gzip.compress(driving_data_json)

            await db.execute(
                text(
                    """
                    INSERT INTO driving_history 
                    (driver_id, driving_data, created_at)
                    VALUES (:driver_id, :driving_data, :created_at);
                    """
                ),
                {
                    "driver_id": driver_id,
                    "driving_data": compressed_driving_data,
                    "created_at": now,
                },
            )
            await db.commit()
    except SQLAlchemyError as e:
        await db.rollback()
        logging.error(f"Database operation failed: {e}")
        raise e
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        raise e


async def post_location_data(
    driver_id: str,
    latitude: float,
    longitude: float,
) -> str | None:
    url = "http://172.25.22.158:8000/service/route/update_route"
    post_data = {
        "driver_id": driver_id,
        "latitude": latitude,
        "longitude": longitude,
    }
    headers = {"Micro-Service-Key": MICRO_SERVICE_KEY}

    # Input validation (if necessary)
    if (
        not driver_id
        or not isinstance(latitude, float)
        or not isinstance(longitude, float)
    ):
        print("Invalid input data")
        return None

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=post_data, headers=headers) as response:
                if response.status == 200:
                    json_response = await response.json()
                    hex_data = json_response.get("polyline_with_instruction", "")
                    if hex_data:
                        # Convert the hexadecimal data back to bytes
                        gzip_data = binascii.unhexlify(hex_data)
                        # Decompress gzip data
                        with gzip.open(io.BytesIO(gzip_data), "rb") as f:
                            decompressed_data = f.read()
                        # Assuming the decompressed data is a string (modify as needed)
                        decoded_data = decompressed_data.decode("utf-8")
                        return decoded_data
                    else:
                        print("No polyline_with_instruction found in response")
                else:
                    print(f"Received non-200 response: {response.status}")
    except aiohttp.ClientError as e:
        print(f"HTTP Client error occurred: {e}")
    except binascii.Error as e:
        print(f"Error decoding hexadecimal data: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

    return None


# Function to get elevation data
def calculate_average_speed_with_timestamps(
    data_points: list[tuple[float, float]],
    start_time: datetime,
    finish_time: datetime,
) -> float:
    # Input validation
    if not data_points or len(data_points) < 2:
        return 0

    if not isinstance(start_time, datetime) or not isinstance(finish_time, datetime):
        raise ValueError("start_time and finish_time must be datetime objects")

    total_time_interval = (finish_time - start_time).total_seconds()
    if total_time_interval <= 0:
        return 0

    try:
        total_distance_km = sum(
            geodesic(data_points[i], data_points[i + 1]).kilometers
            for i in range(len(data_points) - 1)
        )

        if total_distance_km == 0:
            return 0

        average_speed = total_distance_km / (
            total_time_interval / 3600
        )  # Speed in km/h
        return average_speed
    except Exception as e:
        raise RuntimeError(f"Error calculating average speed: {e}")


def is_off_track(
    latitude: float,
    longitude: float,
    polyline_with_instruction: str,
    threshold_distance: int = 100,
) -> bool:
    try:
        current_location = (latitude, longitude)

        # Decode the polyline to get route coordinates
        try:
            route_coordinates = polyline.decode(polyline_with_instruction)
        except Exception as e:
            logging.error(f"Error decoding polyline: {e}")
            return False

        # Convert the route coordinates to a Shapely LineString
        try:
            line = LineString(route_coordinates)
        except Exception as e:
            logging.error(f"Error creating LineString: {e}")
            return False

        # Create a Shapely Point for the current location
        point = Point(current_location)

        # Find the closest point on the line to the current point
        closest_point = line.interpolate(line.project(point))

        # Calculate the geodesic distance from the closest point on the line to the current location
        closest_point_coords = (closest_point.y, closest_point.x)
        distance = geodesic(
            closest_point_coords,
            (longitude, latitude),
        ).meters

        # Check if the distance is greater than the threshold
        return distance > threshold_distance

    except Exception as general_error:
        logging.error(f"General error in is_off_track function: {general_error}")
        return False


def check_if_off_track(
    polyline_with_instruction: str,
    current_latitude: float,
    current_longitude: float,
) -> bool:
    try:
        # Parse the JSON response
        data = json.loads(polyline_with_instruction)

        # Check if the top-level structure is a list and has at least one item
        if isinstance(data, list) and len(data) > 0:
            first_route = data[0]
            if (
                "overview_polyline" in first_route
                and "points" in first_route["overview_polyline"]
            ):
                polyline_with_instruction = first_route["overview_polyline"]["points"]
            else:
                logging.error("Overview polyline not found in the route data")
                return False
        else:
            logging.error("No routes found or invalid JSON structure")
            return False

        # Check if off track
        return is_off_track(
            current_latitude,
            current_longitude,
            polyline_with_instruction,
        )
    except KeyError as e:
        logging.error(f"Key error in JSON parsing: {e}")
        return False
    except json.JSONDecodeError as e:
        logging.error(f"JSON Decode Error: {e}")
        return False
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        return False


@app.websocket("/")
async def websocket_endpoint(
    websocket: WebSocket,
) -> None:
    await websocket.accept()

    try:
        # Fetch the data for the first call
        data = await websocket.receive_json()
        if data is None:
            await websocket.send_text(
                orjson.dumps(
                    {"error": "No data received"},
                ).decode("utf-8")
            )
            await websocket.close()
            return

        # Get the token from the data
        token = data.get("X-Deliver-Auth")

    except json.JSONDecodeError:
        await websocket.send_text(
            orjson.dumps(
                {"error": "Invalid JSON format"},
            ).decode("utf-8")
        )
        await websocket.close()
        return
    except Exception as _:
        await websocket.send_text(
            orjson.dumps(
                {"error": "Error processing token"},
            ).decode("utf-8")
        )
        await websocket.close()
        return

    # Authenticate for the WebSocket
    driver_id = await authenticate_websocket(websocket, token)
    if not driver_id:
        return

    # Fetch the polyline_with_instruction from the database if it exists
    instruction_with_polyline = await get_polyline_with_instruction(driver_id)

    if instruction_with_polyline is None:
        await websocket.send_text(
            orjson.dumps(
                {"error": "No polyline found"},
            ).decode("utf-8")
        )
        await websocket.close()
        return

    await websocket.send_text(
        orjson.dumps(
            {"message": "change", "data": instruction_with_polyline},
        ).decode("utf-8")
    )

    # Initialize variables
    driver_data_points = []
    bundle_data = []
    batch_start_time = None
    start_time = None
    last_polyline_fetch_time = datetime.min

    # Initialize a counter for polyline fetches
    polyline_fetch_count = 0

    while True:
        try:
            # Receive data from the WebSocket
            received_data = await websocket.receive_json()

            # Check for 'reset' field in the received data
            if received_data.get("reset"):
                polyline_fetch_count = 0
                await websocket.send_text(
                    orjson.dumps(
                        {"message": "Polyline fetch count reset"},
                    ).decode("utf-8")
                )
                continue

            # Data reception
            latitude = received_data.get("latitude")
            longitude = received_data.get("longitude")

            # Data validation for longitude and latitude
            if (
                latitude is None
                or longitude is None
                or not -180 <= longitude <= 180
                or not -90 <= latitude <= 90
            ):
                await websocket.send_text(
                    orjson.dumps(
                        {"error": "Invalid latitude or longitude"},
                    ).decode("utf-8")
                )
                continue

            # Fetch the current time for archive purposes and speed calculation
            now = datetime.now()

            if not start_time:
                start_time = batch_start_time = now

            driver_data_points.append((latitude, longitude, now))

            if len(driver_data_points) % DATA_UPDATE_THRESHOLD == 0:
                await update_current_location(driver_id, latitude, longitude)

                bundled_data = await bundle_data_points(
                    driver_data_points[-DATA_UPDATE_THRESHOLD:], batch_start_time, now
                )
                bundle_data.append(bundled_data)
                batch_start_time = now

                if len(driver_data_points) >= DATA_POINTS_THRESHOLD:
                    await insert_driving_history(driver_id, bundle_data, now)
                    driver_data_points = []
                    bundle_data = []
                    start_time = None

            # Calculate the speed and time since last polyline fetch
            now = datetime.now()
            time_since_last_fetch = (now - last_polyline_fetch_time).total_seconds()

            if (
                len(driver_data_points) % 2 == 0
                and polyline_fetch_count < POLYLINE_FETCH_LIMIT
                and check_if_off_track(
                    instruction_with_polyline,
                    latitude,
                    longitude,
                )
            ):
                if polyline_fetch_count >= POLYLINE_FETCH_LIMIT:
                    # Send a limit exceeded message to the user
                    await websocket.send_text(
                        orjson.dumps(
                            {"error": "Fetch limit exceeded"},
                        ).decode("utf-8")
                    )
                    # Take any additional action if needed, like closing the websocket
                    await websocket.close()
                    continue

                if time_since_last_fetch > TIME_BETWEEN_POLYLINE_FETCH:
                    # Fetch new polyline and update the time
                    instruction_with_polyline = await post_location_data(
                        driver_id, latitude, longitude
                    )
                    last_polyline_fetch_time = now
                    # Send updated polyline to WebSocket
                    await websocket.send_text(
                        orjson.dumps(
                            {"message": "change", "data": instruction_with_polyline},
                        ).decode("utf-8")
                    )

            (
                await websocket.send_text(
                    orjson.dumps(
                        {"message": "Ok"},
                    ).decode("utf-8")
                ),
            )

        except WebSocketDisconnect:
            logging.info("WebSocket disconnected")
            break
        except SQLAlchemyError as e:
            logging.error(f"Database error: {e}")
            await websocket.send_text(
                orjson.dumps(
                    {"error": "Database error"},
                ).decode("utf-8")
            )
            await websocket.close()
            break
        except json.JSONDecodeError:
            await websocket.send_text(
                orjson.dumps(
                    {"error": "Invalid JSON format"},
                ).decode("utf-8")
            )
            continue
        except Exception as e:
            logging.error(f"Unexpected error: {e}")
            await websocket.send_text(
                orjson.dumps(
                    {"error": "An unexpected error occurred"},
                ).decode("utf-8")
            )
            await websocket.close()
            break
