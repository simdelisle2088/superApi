from typing import List
from fastapi.responses import ORJSONResponse
import httpx
from sqlalchemy import select
from model.superDeliver.DriverOrderModel import DriverOrder
from utility.util import HERE_API_KEY, AsyncSession
from datetime import datetime, timedelta
from fastapi import HTTPException, status
import logging
from model.LoggingModel import (
    TelemetryRequest,
    Telemetry,
)

async def set_driver_telemetry(
    driver_id: int,
    validated_data: TelemetryRequest,
    db: AsyncSession,
) -> ORJSONResponse:
    """
    Save telemetry data for a driver to the database.

    Args:
        driver_id (int): The ID of the driver.
        validated_data (TelemetryRequest): The validated telemetry data.
        db (AsyncSession): The database session.

    Returns:
        ORJSONResponse: The response indicating success or failure.
    """
    try:
        # Create a new Telemetry instance with the provided data
        telemetry_data = Telemetry(
            driver_id=driver_id,
            order_number=validated_data.order_number,
            store=validated_data.store,
            latitude=validated_data.latitude,
            longitude=validated_data.longitude,
            # created_at will be automatically set by the default get_eastern_time
        )

        # Add the telemetry data to the database
        db.add(telemetry_data)
        await db.commit()

        # Return a success response
        return ORJSONResponse(
            content={"detail": "Ok"},
            status_code=status.HTTP_200_OK,
        )
    except Exception as e:
        # Rollback the transaction in case of an error
        await db.rollback()
        logging.warning(f"Database error in save telemetry: {e}")
        return ORJSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": "Erreur de la base de données."},
        )
    
async def extract_specific_values(json_data):
    """
    Extract values for 'duration', 'length', 'arrival', and 'departure' from JSON data.

    Args:
        json_data (dict): The JSON data as a dictionary.

    Returns:
        dict: A dictionary with the extracted values.
    """
    routes = json_data.get("routes", [])
    extracted_values = {
        "duration": None,
        "length": None,
        "arrival": None,
        "departure": None
    }

    if routes:
        for section in routes[0].get("sections", []):
            summary = section.get("summary", {})
            extracted_values = {
                "duration": summary.get("duration"),
                "length": summary.get("length"),
                "arrival": section.get("arrival", {}).get("time"),
                "departure": section.get("departure", {}).get("time"),
            }
            break  # Since we only need one section, break after the first one

    return extracted_values

async def get_route(
    origin: str,
    destination: str,
    departureTime: str = None,
    stops: list[str] = None,
) -> dict:
    """
    Fetch route information from Here Maps API and extract specific values.

    Args:
        origin (str): The starting point of the route.
        destination (str): The endpoint of the route.
        departureTime (str, optional): The departure time for the route.
        stops (list[str], optional): Intermediate stops along the route.

    Returns:
        dict: Extracted route information.
    """
    url = "https://router.hereapi.com/v8/routes"
    params = {
        "transportMode": "car",
        "destination": destination,
        "return": "summary",
        "apikey": HERE_API_KEY,
        "origin": origin,
    }

    # Add stops (via points)
    if stops:
        for i, stop in enumerate(stops):
            params[f"via{i+1}"] = stop

    if departureTime and departureTime != 'undefined':
        params["departureTime"] = departureTime

    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url, params=params)
            response.raise_for_status()  # Raise exception for non-200 status codes
            json_response = response.json()  # Get JSON response
            extracted_values = await extract_specific_values(json_response)
            return extracted_values
    except httpx.RequestError as error:
        raise HTTPException(status_code=500, detail=f"An error occurred: {error}")
    
async def get_route_info(
    routes: List[str],
    db: AsyncSession
) -> dict:
    """
    Fetch route information for a list of routes and extract specific values.
    """
    orderList = {}
    for route in routes:
        left_before = 0
        arrival = None
        result = await db.execute(
            select(DriverOrder).where(DriverOrder.route == route).order_by(DriverOrder.order_index.asc())
        )
        orders = result.scalars().all()
        count = len(orders)
        store_coords = {
            1: '45.48750046461106,-73.38457638559589',
            2: '45.33034882948999,-73.29479794494063',
            3: '45.35040656602404,-73.68937198884842',
        }
        previous_order = None

        for i, order in enumerate(orders):
            # If order is delivered, update arrival and skip left_before increment
            if order.is_delivered and order.route_started:
                previous_order = order
                arrival = order.arrived_at
                continue

            # Origin and destination handling
            origin = (
                store_coords[order.store] if i == 0
                else f"{previous_order.latitude},{previous_order.longitude}"
            )
            destination = f"{order.latitude},{order.longitude}"
            departure_time = (
                order.updated_at if i == 0
                else (previous_order.delivered_at if previous_order.is_delivered else arrival)
            )

            # Add 2-minute buffer and fetch route info
            departure_time = (departure_time + timedelta(minutes=2)).isoformat()
            request_data = await get_route(
                origin,
                destination,
                departure_time
            ) 
            request_data['position'] = i
            request_data['count'] = count
            request_data['left_before'] = left_before

            arrival = datetime.fromisoformat(request_data['arrival'])
            previous_order = order
            orderList[order.order_number] = request_data

            # Increment left_before only for undelivered orders
            if not order.is_delivered:
                left_before += 1

    return orderList
