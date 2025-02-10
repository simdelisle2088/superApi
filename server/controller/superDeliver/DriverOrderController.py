import json
import traceback
from typing import List
import uuid
from model.superLocator.ItemModel import ItemsInfo
from utility.util import (
    HERE_API_KEY,
    STATE_OF_DEPLOYMENT,
    AsyncSession,
    DEFAULT_DATETIME,
    DEFAULT_IMAGE_FILENAME,
)
from fastapi.responses import ORJSONResponse
from sqlalchemy.future import select
from sqlalchemy.sql import func, text
from datetime import datetime, timedelta
from sqlalchemy import Integer, cast, or_, select, func, and_, delete, update
from sqlalchemy.ext.asyncio import AsyncSession
from starlette import status
from datetime import datetime, timedelta
from fastapi import status, HTTPException
import logging
import asyncio
import orjson
import time
from datetime import datetime
from zoneinfo import ZoneInfo
from zoneinfo import ZoneInfo
import aiohttp
from model.superDeliver.CancelModel import DriverCancelOrder
from model.superDeliver.TentativeModel import (DriverTentativeOrder)
from model.superDeliver import (DriverOrderModel)
from model.LoggingModel import (SkippedPart)
from model.superDeliver.OrderModel import (Order)
from model.QueryModel import QueryParams
from model.superDeliver.DriverModel import (
    Driver, 
    DriverId, 
    DriverIdRoute, 
    StoreId
)
from model.superDeliver.DriverOrderModel import (
    DriverOrder,
    DriverOrderNumber,
    InvoiceCode,
    OrderReorderRequest,
    ScannedBatch,
    ScannedPart,
    InvoiceInfo,
)

# Convert minutes to a formatted string representing hours and minutes
def convert_minutes_to_hours(minutes):
    if minutes is None:
        return None
    hours = int(minutes // 60)
    remaining_minutes = int(minutes % 60)
    return f"{hours}h {remaining_minutes}min"

# Fetch delivery times for all drivers in a store
async def get_all_deliveries_time(store_id: int, db: AsyncSession) -> ORJSONResponse:
    try:
        # Fetch the latest order's creation date for the specified store
        latest_order_query = select(func.max(DriverOrder.created_at))
        if store_id is not None:
            latest_order_query = latest_order_query.where(DriverOrder.store == store_id)

        latest_order_result = await db.execute(latest_order_query)
        latest_created_at = latest_order_result.scalar()
        if not latest_created_at:
            return ORJSONResponse(
                status_code=404,
                content={"detail": "No recent orders found for the given parameters"},
            )

        last_30_days = latest_created_at - timedelta(days=30)
        last_7_days = latest_created_at - timedelta(days=7)
        one_year_ago = latest_created_at - timedelta(days=365)

        # Calculate delivery statistics for a given time period
        async def calculate_delivery_stats(start_time):
            query = select(
                func.avg(
                    func.timestampdiff(
                        text("MINUTE"), DriverOrder.created_at, DriverOrder.delivered_at
                    )
                ).label("avg_delivery_time"),
                func.count().label("order_count"),
            ).where(
                DriverOrder.created_at >= start_time,
                DriverOrder.created_at <= latest_created_at,
                DriverOrder.delivered_at != '1000-01-01 12:00:00',
                DriverOrder.is_delivered == True,
                func.timestampdiff(
                    text("MINUTE"), DriverOrder.created_at, DriverOrder.delivered_at
                ) >= 2
            )

            if store_id is not None:
                query = query.where(DriverOrder.store == store_id)

            results = await db.execute(query)
            avg_delivery_time, order_count = results.first()
            return avg_delivery_time, order_count

        # Fetch statistics for all drivers in the store
        avg_30_days, count_30_days = await calculate_delivery_stats(last_30_days)
        avg_7_days, count_7_days = await calculate_delivery_stats(last_7_days)
        avg_overall, count_overall = await calculate_delivery_stats(one_year_ago)

        delivery_times = {
            "last_30_days": convert_minutes_to_hours(avg_30_days),
            "last_7_days": convert_minutes_to_hours(avg_7_days),
            "overall": convert_minutes_to_hours(avg_overall),
            "count_last_30_days": count_30_days,
            "count_last_7_days": count_7_days,
            "count_overall": count_overall,
        }

        return ORJSONResponse(status_code=200, content=delivery_times)
    except Exception as e:
        # More detailed error handling can be added here if needed
        return ORJSONResponse(status_code=500, content={"detail": str(e)})

# Fetch delivery times for a specific driver in a store
async def get_deliveries_time(store_id: int, driver_id: int, db: AsyncSession) -> ORJSONResponse:
    try:
        # Fetch the latest order's creation date for the specified driver and store
        latest_order_query = select(func.max(DriverOrder.created_at)).where(
            DriverOrder.driver_id == driver_id
        )
        if store_id is not None:
            latest_order_query = latest_order_query.where(DriverOrder.store == store_id)

        latest_order_result = await db.execute(latest_order_query)
        latest_created_at = latest_order_result.scalar()
        if not latest_created_at:
            return ORJSONResponse(
                status_code=404,
                content={"detail": "No recent orders found for the given parameters"},
            )

        last_30_days = latest_created_at - timedelta(days=30)
        last_7_days = latest_created_at - timedelta(days=7)

        # Calculate average delivery time for a given time period
        async def calculate_average_delivery_time(start_time, consider_all_orders=False):
            query = select(
                func.avg(
                    func.timestampdiff(
                        text("MINUTE"), DriverOrder.created_at, DriverOrder.delivered_at
                    )
                ),
                func.count(),
            ).where(
                (DriverOrder.created_at >= start_time)
                & (DriverOrder.created_at <= latest_created_at)
                & (DriverOrder.delivered_at != '1000-01-01 12:00:00')
                & (DriverOrder.is_delivered == True)
            )

            if not consider_all_orders:
                query = query.where(DriverOrder.driver_id == driver_id)

            if store_id is not None:
                query = query.where(DriverOrder.store == store_id)

            results = await db.execute(query)
            avg_delivery_time, count = results.first()
            return avg_delivery_time, count

        # Calculating average delivery times and counts
        avg_30_days, count_30_days = await calculate_average_delivery_time(last_30_days)
        avg_7_days, count_7_days = await calculate_average_delivery_time(last_7_days)
        avg_overall, count_overall = await calculate_average_delivery_time(
            latest_created_at - timedelta(days=365)
        )  # Adjust this duration for overall average

        # Calculate total order count for the store without filtering by driver
        _, count_store_overall = await calculate_average_delivery_time(
            latest_created_at - timedelta(days=365), consider_all_orders=True
        )

        delivery_times = {
            "last_30_days": convert_minutes_to_hours(avg_30_days),
            "last_7_days": convert_minutes_to_hours(avg_7_days),
            "overall": convert_minutes_to_hours(avg_overall),
            "count_last_30_days": count_30_days,
            "count_last_7_days": count_7_days,
            "count_overall": count_store_overall,  # This now reflects the total store orders
        }

        return ORJSONResponse(status_code=200, content=delivery_times)
    except Exception as e:
        return ORJSONResponse(status_code=500, content={"detail": str(e)})

# Prepare order data for response
async def prepare_order_data(orders, secondary_info):

    BASE_URL = {
        "prod": "https://deliver-prod.pasuper.xyz",
        "dev": "https://deliver-dev.pasuper.xyz",
        "local": "http://127.0.0.1:8000",
    }

    image_url_env = BASE_URL.get(STATE_OF_DEPLOYMENT)

    order_data_list = []
    for order, driver_username in orders:
        additional_data = secondary_info.get(order.order_number, {
            "pickers": None, 
            "dispatch": None, 
            "dispatched_at": None, 
            "updated_at": None,
            "ship_addr1": None,
            "ship_addr2": None,
            "ship_addr3": None,
        })

        if additional_data["dispatched_at"] and additional_data["updated_at"]:
            picking_time_seconds = (additional_data["updated_at"] - additional_data["dispatched_at"]).total_seconds()
            picking_minutes, picking_seconds = divmod(picking_time_seconds, 60)

            dispatch_time_seconds = (order.created_at - additional_data["updated_at"]).total_seconds()
            dispatch_minutes, dispatch_seconds = divmod(dispatch_time_seconds, 60)
        else:
            picking_minutes = picking_seconds = dispatch_minutes = dispatch_seconds = 0

        order_data = {}

        # Attributes to be checked in the order object
        attributes_to_check = [
            "client_name", "order_number", "customer", "phone_number", "address",
            "created_at", "arrived_at", "delivered_at", "updated_at", "order_info",
            "order_index", "driver_id", "is_delivered", "photo_filename", "route",
            "latitude", "longitude", "store", "route_started", "received_by"
        ]
        
        # Check attributes in order and add to order_data
        for attr in attributes_to_check:
            if hasattr(order, attr):
                value = getattr(order, attr)
                if isinstance(value, datetime):
                    order_data[attr] = value.strftime("%Y-%m-%d %H:%M:%S")
                elif attr == "photo_filename":
                    for path in ["/home/images-dev", "/home/images", "home/images-dev", "home/images"]:
                        order.photo_filename = order.photo_filename.replace(path, "images")
                    order_data[attr] = f"{image_url_env}/{order.photo_filename}"
                else:
                    order_data[attr] = value
        
        # Additional fields
        if "driver_username" in locals():
            order_data["driver_username"] = driver_username
            order_data["driver_name"] = driver_username
        
        if "pickers" in additional_data:
            order_data["pickers"] = additional_data["pickers"]
        
        if "dispatch" in additional_data:
            order_data["dispatch"] = additional_data["dispatch"]
        
        order_data["avg_picking_time"] = f"{int(picking_minutes)} minutes {int(picking_seconds)} seconds"
        order_data["avg_dispatch_time"] = f"{int(dispatch_minutes)} minutes {int(dispatch_seconds)} seconds"
        
        if "dispatched_at" in additional_data and additional_data["dispatched_at"]:
            order_data["dispatched_at"] = additional_data["dispatched_at"].strftime("%Y-%m-%d %H:%M:%S")
        
        # Additional shipping address fields
        for field in ["ship_addr1", "ship_addr2", "ship_addr3"]:
            if field in additional_data:
                order_data[field] = additional_data[field]
        
        order_data_list.append(order_data)


    return order_data_list
# Fetch secondary information for a list of order numbers from the secondary database
async def fetch_secondary_info(db_secondary: AsyncSession, order_numbers: list) -> dict:
    if not order_numbers:
        return {}

    # Define the query to select relevant fields from the Order table
    query = (
        select(
            Order.pickers,
            Order.dispatch,
            Order.order_number,
            Order.created_at,
            Order.updated_at,
            Order.ship_addr1,
            Order.ship_addr2,
            Order.ship_addr3,
        ).where(Order.order_number.in_(order_numbers))
    )

    # Execute the query and fetch all results
    result = await db_secondary.execute(query)
    rows = result.fetchall()

    # Return a dictionary mapping order numbers to their corresponding secondary information
    return {
        row.order_number: {
            "pickers": row.pickers,
            "dispatch": row.dispatch,
            "dispatched_at": row.created_at,
            "updated_at": row.updated_at,
            "ship_addr1": row.ship_addr1,
            "ship_addr2": row.ship_addr2,
            "ship_addr3": row.ship_addr3,
        }
        for row in rows
    }

# Fetch orders from the primary database and their secondary information
async def fetch_orders(db: AsyncSession, db_secondary: AsyncSession, order, store: int, params: QueryParams):
    # Define the base query to select orders and their associated driver usernames
    query = (select(order, Driver.username).join(Driver, order.driver_id == Driver.id))
    
    # Add conditions to the query based on the provided parameters
    conditions = [order.store == store]
    if params.contain: 
        conditions.append(
            or_(
                order.order_number.like(f'%{params.contain}%'),
                order.client_name.like(f'%{params.contain}%'),
            )
        )
    if params.dateFrom: conditions.append(order.created_at >= params.dateFrom)
    if params.dateTo: conditions.append(order.created_at <= params.dateTo)

    # Apply the conditions and order the results by creation date in descending order
    query = query.filter(and_(*conditions)).order_by(order.created_at.desc())
    
    # Apply offset and limit if provided
    if params.offset: query = query.offset(params.offset)
    if params.count: query = query.limit(params.count)

    # Execute the query and fetch all results
    result_orders = await db.execute(query)
    orders = result_orders.all()

    # Extract order numbers from the results
    order_numbers = [order.order_number for order, _ in orders]

    # Fetch secondary information for the orders
    secondary_info_orders = await fetch_secondary_info(db_secondary, order_numbers)

    # Prepare and return the order data
    return await prepare_order_data(orders, secondary_info_orders)

# Get all orders for a specific driver and route number
async def get_all_routes_by_driverId(validated_data: DriverIdRoute, db: AsyncSession) -> ORJSONResponse:
    try:
        # Fetch all orders by driver_id and route number
        result = await db.execute(
            select(DriverOrder)
            .where(
                DriverOrder.driver_id == validated_data.driver_id,
                DriverOrder.route == validated_data.route_number
            )
        )
        orders = result.scalars().all()

        if not orders:
            return ORJSONResponse(status_code=status.HTTP_200_OK, content=[])

        # Prepare the response data
        orders_data = [
            {
                "id": order.id,
                "order_number": order.order_number,
                "store": order.store,
                "customer": order.customer,
                "order_info": order.order_info,
                "client_name": order.client_name,
                "phone_number": order.phone_number,
                "latitude": order.latitude,
                "longitude": order.longitude,
                "address": order.address,
                "ship_addr": order.ship_addr,
                "driver_id": order.driver_id,
                "photo_filename": order.photo_filename,
                "is_arrived": order.is_arrived,
                "is_delivered": order.is_delivered,
                "created_at": order.created_at.strftime("%Y-%m-%d %H:%M:%S"),
                "arrived_at": order.arrived_at.strftime("%Y-%m-%d %H:%M:%S") if order.arrived_at else None,
                "delivered_at": order.delivered_at.strftime("%Y-%m-%d %H:%M:%S") if order.delivered_at else None,
                "updated_at": order.updated_at.strftime("%Y-%m-%d %H:%M:%S"),
                "driver_name": order.driver_name,
                "order_index": order.order_index,
                "route": order.route,
                "route_started": order.route_started,
                "received_by": order.received_by,
            }
            for order in orders
        ]

        # Add the average delivery time to the response
        response_data = {
            "orders": orders_data,
        }

        return ORJSONResponse(
            status_code=status.HTTP_200_OK,
            content=response_data,
        )

    except Exception as e:
        logging.error(f"Error fetching all orders by route: {e}")
        return ORJSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": "Unknown error occurred while retrieving orders by route."},
        )

# Get all orders for a specific driver ID
async def get_all_orders_by_id(validated_data: DriverId, db: AsyncSession, db_secondary: AsyncSession) -> ORJSONResponse:
    try:
        # Stream orders from primary database
        order_stream = await db.stream(
            select(DriverOrder)
            .where(DriverOrder.driver_id == validated_data.driver_id)
            .order_by(DriverOrder.order_number.asc())
            .execution_options(yield_per=25) 
        )
        
        orders = []
        order_numbers = []

        # Collect orders and their numbers from the stream
        async for row in order_stream:
            order = row[0]  # Ensure you're accessing the correct tuple element
            orders.append(order)
            order_numbers.append(order.order_number)

        if not orders:
            return ORJSONResponse(status_code=status.HTTP_200_OK, content=[])

        # Fetch ship_addr1 from the secondary database using the order numbers
        secondary_result = await db_secondary.execute(
            select(Order.order_number, Order.ship_addr1)
            .where(Order.order_number.in_(order_numbers))
        )
        secondary_data = {row.order_number: row.ship_addr1 for row in secondary_result.all()}

        # Prepare the response data
        orders_data = [
            {
                "client_name": order.client_name,
                "order_number": order.order_number,
                "customer": order.customer,
                "phone_number": order.phone_number,
                "address": order.address,
                "latitude": order.latitude,
                "longitude": order.longitude,
                "is_delivered": order.is_delivered,
                "created_at": order.created_at.strftime("%Y-%m-%d %H:%M:%S"),
                "arrived_at": order.arrived_at.strftime("%Y-%m-%d %H:%M:%S") if order.arrived_at else None,
                "delivered_at": order.delivered_at.strftime("%Y-%m-%d %H:%M:%S") if order.delivered_at else None,
                "updated_at": order.updated_at.strftime("%Y-%m-%d %H:%M:%S"),
                "order_info": order.order_info,
                "driver_id": order.driver_id,
                "route": order.route,
                "route_started": order.route_started,
                "received_by": order.received_by,
                "price": order.price,
                "ship_addr1": secondary_data.get(order.order_number),  # Add ship_addr1 to the response
            }
            for order in orders
        ]

        # Calculate the average overall delivery time for all orders
        avg_delivery_time_query = select(
            func.avg(
                func.timestampdiff(
                    text("MINUTE"), DriverOrder.created_at, DriverOrder.delivered_at
                )
            ).label("avg_delivery_time")
        ).where(
            DriverOrder.driver_id == validated_data.driver_id,
            DriverOrder.delivered_at != '1000-01-01 12:00:00',
            DriverOrder.is_delivered == True
        )

        avg_delivery_time_result = await db.execute(avg_delivery_time_query)
        avg_delivery_time = avg_delivery_time_result.scalar()

        # Add the average delivery time to the response
        response_data = {
            "orders": orders_data,
            "average_delivery_time_hours": convert_minutes_to_hours(avg_delivery_time)
        }

        return ORJSONResponse(
            status_code=status.HTTP_200_OK,
            content=response_data,
        )

    except Exception as e:
        logging.error(f"Error fetching all orders: {e}")
        return ORJSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": "Unknown error occurred while retrieving orders."},
        )

# Get all undelivered orders for a specific driver
async def get_driver_orders(
    driver_id: int,
    db: AsyncSession,
) -> ORJSONResponse:
    try:
        # Fetch all undelivered orders for the driver
        result = await db.execute(
            select(DriverOrder)
            .where(
                and_(
                    DriverOrder.driver_id == driver_id,
                    DriverOrder.is_delivered == 0,
                )
            )
            .order_by(DriverOrder.order_index.asc())
        )

        driver_orders = result.scalars().all()

        if not driver_orders:
            return ORJSONResponse(status_code=status.HTTP_200_OK, content=[])

        # Prepare the response data
        orders_data = [
            {
                "tracking_number": order.tracking_number,
                "order_number": order.order_number,
                "store": order.store,
                "customer": order.customer,
                "order_info": order.order_info,
                "client_name": order.client_name,
                "phone_number": order.phone_number,
                "address": order.address,
                "driver_id": order.driver_id,
                "is_arrived": order.is_arrived,
                "latitude": order.latitude,
                "longitude": order.longitude,
                "order_index": order.order_index,
                "route": order.route, 
                "route_started": order.route_started,
                "received_by": order.received_by,
                "price": order.price,
                "job": order.job
            }
            for order in driver_orders
        ]

        return ORJSONResponse(
            status_code=status.HTTP_200_OK,
            content=orders_data,
        )

    except Exception as e:
        logging.error(f"Error fetching driver orders: {e}")
        return ORJSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={
                "detail": "Unknown error occurred while retrieving driver orders."
            },
        )

# Get the current route number for a specific driver
async def get_routes_order(
        driver_id: int,
        db: AsyncSession
    ) -> ORJSONResponse:
    try:
        # Fetch the latest route number for the driver
        route_query = (
            select(DriverOrder.route)
            .where(DriverOrder.driver_id == driver_id)
            # Use SUBSTRING to extract the number after the dash and cast it to an integer
            .order_by(func.cast(func.substring(DriverOrder.route, '^[0-9]+-([0-9]+)$'), Integer).desc())
            .limit(1)
        )
        route_result = await db.execute(route_query)
        current_route = route_result.scalar_one_or_none()

        # If there are no orders yet, start with route 1
        route = 1 if current_route is None else current_route

        # Return the response with the current route number
        return ORJSONResponse(
            content={"route": route},
            status_code=status.HTTP_200_OK,
        )
    except Exception as e:
        # Log the error and return a 500 response
        logging.error(f"Unexpected error occurred: {str(e)}")
        return ORJSONResponse(
            content={"detail": "An error occurred while fetching the route number."},
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
    
async def cancel_driver_orders(
    validated_data: InvoiceInfo,
    db: AsyncSession,
    is_dispatch: bool = False,
) -> ORJSONResponse:
    try:
        # Fetch the existing order's data
        fetch_query = select(DriverOrder).where(
            and_(
                DriverOrder.order_number == validated_data.orderNumber,
                DriverOrder.store == validated_data.store,
                DriverOrder.is_delivered == 0 if not is_dispatch else True
            )
        )
        result = await db.execute(fetch_query)
        existing_order = result.scalar_one_or_none()

        if not existing_order:
            logging.warning("Order not found or insufficient privileges.")
            return ORJSONResponse(
                content={"detail": "Commande non trouvée."},
                status_code=status.HTTP_404_NOT_FOUND,
            )

        # Prepare and execute the delete query
        delete_query = delete(DriverOrder).where(
            and_(
                DriverOrder.order_number == validated_data.orderNumber,
                DriverOrder.store == validated_data.store,
                DriverOrder.is_delivered == 0 if not is_dispatch else True
            )
        )
        await db.execute(delete_query)

        # Get current time in Eastern Time
        eastern = ZoneInfo('America/New_York')
        cancel_at = datetime.now(eastern)

        # Prepare the new entry for cancel_orders
        cancel_order = DriverCancelOrder(
            order_number=existing_order.order_number,
            store=existing_order.store,
            customer=existing_order.customer,
            order_info=existing_order.order_info,
            client_name=existing_order.client_name,
            phone_number=existing_order.phone_number,
            latitude=existing_order.latitude,
            longitude=existing_order.longitude,
            address=existing_order.address,
            driver_id=existing_order.driver_id,
            created_at=existing_order.created_at,
            cancel_at=cancel_at,
            driver_name=existing_order.driver_name,
            route=existing_order.route,
            received_by=existing_order.received_by,
            active=1
        )

        db.add(cancel_order)
        await db.commit()

        return ORJSONResponse(
            content={"detail": "Commande retourner et réécrite avec succès."},
            status_code=status.HTTP_200_OK,
        )
    except Exception as e:
        logging.error(f"Unexpected error occurred: {str(e)}")
        await db.rollback()
        return ORJSONResponse(
            content={
                "detail": "Erreur inconnue lors de la suppression de la commande."
            },
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )

async def fetch_clean_addresses(
    address1: str,
    address2: str,
    address3: str,
    ship_addr1: str,
    ship_addr2: str,
    ship_addr3: str,
) -> tuple[str, float, float]:

    def create_query_address(addr1, addr2, addr3):
        return f"{addr1}, {addr2}, {addr3}"

    def contains_psl(*args):
        return any("PSL" in addr for addr in args if addr)

    def extract_address_with_postal_code(*args):
        for addr in args:
            if addr and any(char.isdigit() for char in addr):
                return addr
        return "Unknown address"

    if contains_psl(address1, address2, address3):
        if contains_psl(ship_addr1, ship_addr2, ship_addr3):
            query_address = extract_address_with_postal_code(
                address1, address2, address3, ship_addr1, ship_addr2, ship_addr3
            ) or create_query_address(ship_addr1, ship_addr2, ship_addr3)
        else:
            query_address = create_query_address(ship_addr1, ship_addr2, ship_addr3)
    else:
        if contains_psl(ship_addr1, ship_addr2, ship_addr3):
            query_address = create_query_address(address1, address2, address3)
        else:
            query_address = create_query_address(address1, address2, address3)

    base_url = "https://geocode.search.hereapi.com/v1/discover"
    params = {
        "q": query_address,
        "apiKey": HERE_API_KEY,
        "in": "bbox:-79.7624,45.0118,-57.1103,63.0001",
    }

    async with aiohttp.ClientSession() as session:
        async with session.get(base_url, params=params) as response:
            if response.status == 200:
                results = await response.json()
                items = results.get("items", [])
                if items:
                    primary_result = items[0]
                    clean_address = primary_result.get("title")
                    position = primary_result.get("position")
                    latitude = position.get("lat")
                    longitude = position.get("lng")
                    return clean_address, latitude, longitude

    return "Unknown address", 0.0, 0.0

async def fetch_existing_orders_for_driver(client_name: str, route: str, db: AsyncSession):
    try:
        # Query to fetch existing orders for a specific driver and route
        existing_orders_query = (
            select(DriverOrder)
            .where(
                and_(
                    DriverOrder.client_name == client_name,
                    DriverOrder.route == route,
                )
            )
        )
        existing_orders_result = await db.execute(existing_orders_query)
        existing_orders = existing_orders_result.scalars().all()
        return existing_orders
    except Exception as e:
        print(f"Error fetching existing orders: {e}")
        return []

def merge_order_info(new_order_info: str, existing_orders: list) -> str:
    # Load new order information from JSON
    all_items = orjson.loads(new_order_info)
    merged_items = {}

    # Merge new order items
    for item in all_items:
        key = item['item']
        if key in merged_items:
            merged_items[key]['units'] += item['units']
        else:
            merged_items[key] = item

    # Merge existing order items
    for order in existing_orders:
        items = orjson.loads(order.order_info)
        for item in items:
            key = item['item']
            if key in merged_items:
                merged_items[key]['units'] += item['units']
            else:
                merged_items[key] = item

    # Convert merged items back to JSON
    merged_items_list = list(merged_items.values())
    return orjson.dumps(merged_items_list).decode("utf-8")

async def delete_existing_orders(existing_orders: list, db: AsyncSession):
    # Delete each existing order from the database
    for order in existing_orders:
        await db.delete(order)
        
def process_order_info(order_info: str) -> str:
    """
    Process the order information by converting it from JSON string to a list of dictionaries,
    and then back to a JSON string with additional fields.

    Args:
        order_info (str): JSON string containing order information.

    Returns:
        str: Processed order information as a JSON string.
    """
    new_order_info = []
    for item in orjson.loads(order_info):
        new_order_info.append(
            {
                "item": item["item"],
                "description": item["description"],
                "units": int(item["units"]),
                "num_scanned": int(0),
                "confirmed_scanned": int(0),
            }
        )
    return orjson.dumps(new_order_info).decode("utf-8")

def create_new_driver_order(
    driver_id: int, driver_username: str, new_order_data, address, latitude, longitude, new_order_info, created_at, new_order_index, route: int, received_by: str, price: str
) -> DriverOrder:
    """
    Create a new DriverOrder instance with the provided data.

    Args:
        driver_id (int): ID of the driver.
        driver_username (str): Username of the driver.
        new_order_data: Data for the new order.
        address (str): Address for the order.
        latitude (float): Latitude of the delivery location.
        longitude (float): Longitude of the delivery location.
        new_order_info (str): Processed order information.
        created_at (datetime): Creation time of the order.
        new_order_index (int): Index of the new order.
        route (int): Route number.
        received_by (str): Name of the person who received the order.
        price (str): Price of the order.

    Returns:
        DriverOrder: A new DriverOrder instance.
    """
    return DriverOrder(
        order_number=new_order_data.order_number,
        store=new_order_data.store,
        customer=new_order_data.customer,
        order_info=new_order_info,
        client_name=new_order_data.client_name,
        phone_number=new_order_data.phone_number,
        driver_id=driver_id,
        driver_name=driver_username,  # Add the driver's username to driver_name column
        latitude=latitude,
        longitude=longitude,
        address=address,
        ship_addr=address,
        photo_filename=DEFAULT_IMAGE_FILENAME,
        is_arrived=False,
        is_delivered=False,
        created_at=created_at,
        arrived_at=DEFAULT_DATETIME,
        delivered_at=DEFAULT_DATETIME,
        order_index=new_order_index,
        route=route,
        received_by=received_by,
        price=price,
    )

async def reorder_driver_orders(
        driver_id: str,
            validated_data: List[OrderReorderRequest],
            db: AsyncSession
        ) -> ORJSONResponse:
        try:
            logging.info(f"Received data: {validated_data} for driver_id: {driver_id}")

            # Fetch the existing orders for the specific driver from the database
            result = await db.execute(
                select(DriverOrderModel.DriverOrder).filter_by(driver_id=driver_id).order_by(DriverOrderModel.DriverOrder.order_index.asc())
            )
            existing_orders = result.scalars().all()

            existing_order_dict = {order.order_number: order for order in existing_orders}
            new_index_mapping = {order_request.order_number: order_request.new_index for order_request in validated_data}

            for order_number, new_index in new_index_mapping.items():
                if order_number in existing_order_dict:
                    existing_order_dict[order_number].order_index = new_index

            # Filter out orders with NoneType order_index and sort the remaining orders
            valid_orders = [order for order in existing_orders if order.order_index is not None]
            sorted_orders = sorted(valid_orders, key=lambda order: order.order_index)

            # Ensure order_index is sequential and starts from 1
            for index, order in enumerate(sorted_orders, start=1):
                order.order_index = index

            await db.commit()
            return ORJSONResponse({"detail": "Order updated successfully."})

        except Exception as e:
            logging.error(f"Error reordering driver orders: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Error reordering driver orders: {e}"
        )


async def merge_orders_for_client(
    driver_id: int,
    customer_number: str,
    new_order_info: str,
    new_order_number: str,
    db: AsyncSession,
) -> tuple[str, str]:  # Return the merged order_info and the tracking number

    # Check if there are any undelivered, merged orders for the same client and driver
    latest_merged_order_query = (
        select(DriverOrder)
        .where(
            and_(
                DriverOrder.customer == customer_number,
                DriverOrder.driver_id == driver_id,
                DriverOrder.is_delivered == False
            )
        )
        .order_by(DriverOrder.created_at.desc())
        .limit(1)
    )
    latest_merged_order_result = await db.execute(latest_merged_order_query)
    latest_merged_order = latest_merged_order_result.scalar_one_or_none()

    # Initialize merged order info
    merged_order_info = {}

    # If a merged order already exists, load its order_info into merged_order_info
    if latest_merged_order:
        try:
            existing_merged_info = json.loads(latest_merged_order.order_info)
            for item in existing_merged_info:
                part_number = item['item']
                merged_order_info[part_number] = item
            print(f"Loaded existing merged items. Total items now: {len(merged_order_info)}")
        except json.JSONDecodeError:
            logging.warning("Failed to decode existing merged order_info JSON")

    # Process the new order info and add it to merged_order_info
    try:
        new_order_info_data = json.loads(new_order_info)
        for item in new_order_info_data:
            part_number = item['item']
            item['order_number'] = new_order_number
            
            # If item already exists, update its fields
            if part_number in merged_order_info:
                existing_item = merged_order_info[part_number]
                existing_item['units'] += item['units']
                
                # Retain and update 'num_scanned' and 'confirmed_scanned' if they exist
                existing_item['num_scanned'] = existing_item.get('num_scanned', 0) + item.get('num_scanned', 0)
                existing_item['confirmed_scanned'] = existing_item.get('confirmed_scanned', 0) + item.get('confirmed_scanned', 0)
            else:
                # Add the item directly if it doesn't exist
                merged_order_info[part_number] = item

        print(f"Added items from new order {new_order_number}. Total items now: {len(merged_order_info)}")
    except json.JSONDecodeError:
        logging.warning(f"JSONDecodeError for new order {new_order_number}, skipping...")

    # Convert merged order_info back to JSON string
    merged_order_info_json = json.dumps(list(merged_order_info.values()))

    # Generate a new tracking number for the merged order
    if latest_merged_order:
        new_tracking_number = latest_merged_order.order_number  # Use existing tracking number if it exists
    else:
        new_tracking_number = f"{driver_id}-{int(time.time())}"  # Create a new tracking number

    return merged_order_info_json, new_tracking_number






# Helper function to get the current route number
async def get_current_route(driver_id: int, db: AsyncSession) -> int:
    # Adjust the SQL query to ensure the numeric part is treated correctly
    route_query = (
        select(DriverOrder.route)
        .where(DriverOrder.driver_id == driver_id)
        .order_by(
            # Ensure the numeric part after '-' is treated as an integer
            cast(func.substring_index(DriverOrder.route, '-', -1), Integer).desc()
        )
        .limit(1)
    )
    route_result = await db.execute(route_query)
    current_route = route_result.scalar_one_or_none()

    if current_route:
        try:
            # Debug: print or log current_route to ensure it's correct
            print(f"Debug - Current route: {current_route}")
            
            # Split and get the last part after the '-'
            last_part = current_route.split("-")[1]
            
            # Convert the extracted number to an integer
            next_route_number = int(last_part) + 1
            return next_route_number
        except (ValueError, IndexError):
            # If parsing fails, return default route
            return 1
    return 1
    
async def set_to_arrived(
    validated_data: InvoiceInfo,
    db: AsyncSession,
) -> ORJSONResponse:
    """
    Mark an order as arrived.

    Args:
        validated_data (InvoiceInfo): Validated data containing order number and store.
        db (AsyncSession): Database session.

    Returns:
        ORJSONResponse: Response indicating the result of the operation.
    """
    try:
        result = await db.execute(
            select(DriverOrder)
            .where(
                and_(
                    DriverOrder.order_number == validated_data.orderNumber,
                    DriverOrder.store == validated_data.store,
                )
            )
            .limit(1)
        )

        driver_order = result.scalars().first()

        if not driver_order:
            return ORJSONResponse(
                status_code=status.HTTP_404_NOT_FOUND,
                content={"detail": "Commande non trouvée."},
            )

        # Get the current time in Eastern Time
        eastern = ZoneInfo('America/New_York')
        created_at = datetime.now(eastern)

        driver_order.is_arrived = True
        driver_order.arrived_at = created_at

        await db.commit()
        await db.refresh(driver_order)

        return ORJSONResponse(
            status_code=status.HTTP_200_OK,
            content={"detail": "Commande marquée comme arrivé."},
        )

    except Exception as e:
        logging.error(f"Error fetching driver orders: {e}")
        return ORJSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": "Erreur inconue lors de la requête."},
        )

async def set_to_delivered(
    validated_data: InvoiceInfo,
    db: AsyncSession,
) -> ORJSONResponse:
    """
    Mark an order as delivered.

    Args:
        validated_data (InvoiceInfo): Validated data containing order number and store.
        db (AsyncSession): Database session.

    Returns:
        ORJSONResponse: Response indicating the result of the operation.
    """
    try:
        result = await db.execute(
            select(DriverOrder)
            .where(
                and_(
                    DriverOrder.order_number == validated_data.orderNumber,
                    DriverOrder.store == validated_data.store,
                )
            )
            .limit(1)
        )

        driver_order = result.scalars().first()

        if not driver_order:
            return ORJSONResponse(
                status_code=status.HTTP_404_NOT_FOUND,
                content={"detail": "Commande non trouvée."},
            )

        if driver_order.photo_filename == DEFAULT_IMAGE_FILENAME:
            return ORJSONResponse(
                status_code=status.HTTP_400_BAD_REQUEST,
                content={"detail": "Photo de livraison manquante."},
            )

        order_info = orjson.loads(driver_order.order_info)
        for item in order_info:
            if item["confirmed_scanned"] < item["units"]:
                return ORJSONResponse(
                    content={
                        "detail": "Impossible de marquée comme livrée, car l'ensemble des pièces n'a pas été scanné une deuxième fois."
                    },
                    status_code=status.HTTP_409_CONFLICT,
                )

        if driver_order.is_arrived is False:
            logging.warning(
                "Driver is not set to arrived but all parts where confirmed. This should not happen."
            )
            return ORJSONResponse(
                status_code=status.HTTP_409_CONFLICT,
                content={"detail": "Commande n'est pas marqué comme arrivé."},
            )

        # Get the current time in Eastern Time
        eastern = ZoneInfo('America/New_York')
        delivered_at = datetime.now(eastern)

        driver_order.is_delivered = True
        driver_order.delivered_at = delivered_at

        await db.commit()
        await db.refresh(driver_order)

        return ORJSONResponse(
            status_code=status.HTTP_200_OK,
            content={"detail": "Commande marquée comme livrée."},
        )

    except Exception as e:
        logging.error(f"Error fetching driver orders: {e}")
        return ORJSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": "Erreur inconue lors de la requête."},
        )

async def remove_driver_order(
    validated_data: InvoiceInfo,
    db: AsyncSession,
    is_dispatch: bool = False,
) -> ORJSONResponse:
    """
    Remove a driver order from the database.

    Args:
        validated_data (InvoiceInfo): Validated data containing order number and store.
        db (AsyncSession): Database session.
        is_dispatch (bool): Flag indicating if the order is dispatched.

    Returns:
        ORJSONResponse: Response indicating the result of the operation.
    """
    try:
        delete_query = None

        # Prepare and execute the delete query
        if is_dispatch:
            delete_query = delete(DriverOrder).where(
                and_(
                    DriverOrder.order_number == validated_data.orderNumber,
                    DriverOrder.store == validated_data.store,
                )
            )
        else:
            delete_query = delete(DriverOrder).where(
                and_(
                    DriverOrder.order_number == validated_data.orderNumber,
                    DriverOrder.store == validated_data.store,
                    DriverOrder.is_delivered == 0,
                )
            )

        result = await db.execute(delete_query)

        # Check if any rows were affected
        if result.rowcount == 0:
            logging.warning("Order not found or insufficient privileges.")
            return ORJSONResponse(
                content={"detail": "Commande non trouvée."},
                status_code=status.HTTP_404_NOT_FOUND,
            )

        # Commit the transaction
        await db.commit()
        return ORJSONResponse(
            content={"detail": "Commande supprimée avec succès."},
            status_code=status.HTTP_200_OK,
        )
    except Exception as e:
        logging.error(f"Unexpected error occurred: {str(e)}")
        await db.rollback()
        return ORJSONResponse(
            content={
                "detail": "Erreur inconnue lors de la suppression de la commande."
            },
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
    
async def remove_cancel_order(
    validated_data: InvoiceInfo,
    db: AsyncSession,
    is_dispatch: bool = False,
) -> ORJSONResponse:
    try:
        delete_query = None

        # Prepare and execute the delete query to mark the order as inactive
        delete_query = update(DriverCancelOrder).where(
            and_(
                DriverCancelOrder.order_number == validated_data.orderNumber,
                DriverCancelOrder.store == validated_data.store,
            )
        ).values(active=0)

        result = await db.execute(delete_query)

        # Check if any rows were affected by the delete query
        if result.rowcount == 0:
            logging.warning("Order not found or insufficient privileges.")
            return ORJSONResponse(
                content={"detail": "Commande non trouvée."},
                status_code=status.HTTP_404_NOT_FOUND,
            )

        # Commit the transaction to save changes
        await db.commit()
        return ORJSONResponse(
            content={"detail": "Commande supprimée avec succès."},
            status_code=status.HTTP_200_OK,
        )
    except Exception as e:
        logging.error(f"Unexpected error occurred: {str(e)}")
        await db.rollback()  # Rollback the transaction in case of error
        return ORJSONResponse(
            content={
                "detail": "Erreur inconnue lors de la suppression de la commande."
            },
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )

async def retour_driver_orders(
    validated_data: InvoiceInfo,
    db: AsyncSession,
    is_dispatch: bool = False,
) -> ORJSONResponse:
    try:
        # Fetch the existing order's data
        fetch_query = select(DriverOrder).where(
            and_(
                DriverOrder.order_number == validated_data.orderNumber,
                DriverOrder.store == validated_data.store,
                DriverOrder.is_delivered == 0 if not is_dispatch else True
            )
        )
        result = await db.execute(fetch_query)
        existing_order = result.scalar_one_or_none()

        # Check if the order exists
        if not existing_order:
            logging.warning("Order not found or insufficient privileges.")
            return ORJSONResponse(
                content={"detail": "Commande non trouvée."},
                status_code=status.HTTP_404_NOT_FOUND,
            )

        # Prepare and execute the delete query to remove the order
        delete_query = delete(DriverOrder).where(
            and_(
                DriverOrder.order_number == validated_data.orderNumber,
                DriverOrder.store == validated_data.store,
                DriverOrder.is_delivered == 0 if not is_dispatch else True
            )
        )
        await db.execute(delete_query)

        # Get current time in Eastern Time
        eastern = ZoneInfo('America/New_York')
        tentative_at = datetime.now(eastern)

        # Prepare the new entry for tentative_orders
        tentative_order = DriverTentativeOrder(
            order_number=existing_order.order_number,
            store=existing_order.store,
            customer=existing_order.customer,
            order_info=existing_order.order_info,
            client_name=existing_order.client_name,
            phone_number=existing_order.phone_number,
            latitude=existing_order.latitude,
            longitude=existing_order.longitude,
            address=existing_order.address,
            driver_id=existing_order.driver_id,
            created_at=existing_order.created_at,
            tentative_at=tentative_at,
            driver_name=existing_order.driver_name,
            route=existing_order.route,
            received_by=existing_order.received_by
        )

        db.add(tentative_order)  # Add the tentative order to the database
        await db.commit()  # Commit the transaction to save changes

        return ORJSONResponse(
            content={"detail": "Commande retourner et réécrite avec succès."},
            status_code=status.HTTP_200_OK,
        )
    except Exception as e:
        logging.error(f"Unexpected error occurred: {str(e)}")
        await db.rollback()  # Rollback the transaction in case of error
        return ORJSONResponse(
            content={
                "detail": "Erreur inconnue lors de la suppression de la commande."
            },
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )

async def scan_part(
    validated_data: ScannedPart,
    db: AsyncSession,
    scan_type: str = "num_scanned",
) -> ORJSONResponse:
    try:
        # We only need one database connection now
        # Prepare queries to fetch item from inventory and driver order
        inventory_stmt = select(ItemsInfo).where(ItemsInfo.upc == validated_data.partCode).limit(1)
        driver_order_stmt = (
            select(DriverOrder)
            .where(
                and_(
                    DriverOrder.order_number == validated_data.orderNumber,
                    DriverOrder.store == validated_data.store,
                    DriverOrder.is_delivered == 0,
                )
            )
            .limit(1)
        )

        # Execute queries concurrently - now both using the same db connection
        inventory_result, driver_order_result = await asyncio.gather(
            db.execute(inventory_stmt),
            db.execute(driver_order_stmt),
        )

        driver_order = driver_order_result.scalar_one_or_none()
        if scan_type == "confirmed_scanned" and driver_order.is_arrived is False:
            return ORJSONResponse(
                content={
                    "detail": "Impossible de confirmer, car le livreur n'est pas marqué comme arrivé."
                },
                status_code=status.HTTP_409_CONFLICT,
            )

        # Check if the driver order exists
        if driver_order is None:
            logging.warning(
                f"Driver order not found for order number: {validated_data.orderNumber}"
            )
            return ORJSONResponse(
                content={"detail": "Commande non trouvée."},
                status_code=status.HTTP_404_NOT_FOUND,
            )

        inventory_item = inventory_result.scalar_one_or_none()
        # Check if the item exists in inventory
        if inventory_item is None:
            logging.warning(f"Part not found in inventory: {validated_data.orderNumber}")
            return ORJSONResponse(
                content={"detail": "Article non trouvé."},
                status_code=status.HTTP_404_NOT_FOUND,
            )

        order_info = orjson.loads(driver_order.order_info)
        for item in order_info:
            # Now using the item field from inventory instead of part_number
            if item["item"] == inventory_item.item:
                if (
                    scan_type == "confirmed_scanned"
                    and item["num_scanned"] < item["units"]
                ):
                    return ORJSONResponse(
                        content={
                            "detail": "Impossible de confirmer, car les pièces n'ont pas toute été scanné une première fois."
                        },
                        status_code=status.HTTP_409_CONFLICT,
                    )

                if item[scan_type] >= item["units"]:
                    return ORJSONResponse(
                        content={
                            "detail": f"Toutes les unités de {item['item']} ont déjà été scannées."
                        },
                        status_code=status.HTTP_409_CONFLICT,
                    )

                item[scan_type] = item[scan_type] + 1
                driver_order.order_info = orjson.dumps(order_info).decode("utf-8")
                await db.commit()
                await db.refresh(driver_order)

                return ORJSONResponse(
                    content={
                        scan_type: item[scan_type],
                        "item": inventory_item.item,  # Now returning item instead of part_number
                    },
                    status_code=status.HTTP_200_OK,
                )

        logging.warning(
            f"Item {inventory_item.item} not found in order number: {validated_data.orderNumber}"
        )
        return ORJSONResponse(
            content={"detail": "Article non trouvé dans la commande."},
            status_code=status.HTTP_404_NOT_FOUND,
        )
    except Exception as error:
        await db.rollback()
        logging.exception(
            f"Exception occurred for order number {validated_data.orderNumber}: {error}"
        )
        return ORJSONResponse(
            content={"detail": "Erreur inconnue lors du scan de la pièce."},
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )

async def skip_part_at_delivery(
    driver_id: int,
    validated_data: ScannedBatch,
    db: AsyncSession,
) -> ORJSONResponse:
    try:
        # Prepare query to fetch the driver order
        stmt = (
            select(DriverOrder)
            .where(
                and_(
                    DriverOrder.order_number == validated_data.orderNumber,
                    DriverOrder.store == validated_data.store,
                    DriverOrder.is_delivered == 0,
                )
            )
            .limit(1)
        )
        result = await db.execute(stmt)
        driver_order = result.scalar_one_or_none()

        # Check if the driver order exists
        if driver_order is None:
            logging.warning(
                f"Driver order not found for order number: {validated_data.orderNumber}"
            )
            return ORJSONResponse(
                content={"detail": "Commande non trouvée."},
                status_code=status.HTTP_404_NOT_FOUND,
            )

        # Check if the driver has arrived
        if driver_order.is_arrived is False:
            return ORJSONResponse(
                content={
                    "detail": "Impossible de confirmer, car le livreur n'est pas marqué comme arrivé."
                },
                status_code=status.HTTP_409_CONFLICT,
            )

        order_info = orjson.loads(driver_order.order_info)

        for item in order_info:
            if item["item"] == validated_data.partNumber:
                # Check if all units of the item have already been confirmed
                if item["confirmed_scanned"] >= item["units"]:
                    return ORJSONResponse(
                        content={
                            "detail": "Toutes les unités de la pièce ont déjà été confirmé."
                        },
                        status_code=status.HTTP_409_CONFLICT,
                    )

                # Record the skipped part
                skipped_part_record = SkippedPart(
                    driver_id=driver_id,
                    order_number=validated_data.orderNumber,
                    store=validated_data.store,
                    part_number=validated_data.partNumber,
                    created_at=datetime.utcnow(),
                )

                db.add(skipped_part_record)  # Add the skipped part record to the database

                # Mark all units of the item as confirmed scanned
                item["confirmed_scanned"] = item["units"]
                driver_order.order_info = orjson.dumps(order_info).decode("utf-8")
                await db.commit()  # Commit the transaction to save changes
                await db.refresh(driver_order)  # Refresh the driver order instance

                return ORJSONResponse(
                    content={"confirmed_scanned": item["confirmed_scanned"]},
                    status_code=status.HTTP_200_OK,
                )

        logging.warning(
            f"Part number {validated_data.partNumber} not found in order number: {validated_data.orderNumber}"
        )
        return ORJSONResponse(
            content={"detail": "Pièce non trouvée dans la commande."},
            status_code=status.HTTP_404_NOT_FOUND,
        )
    except Exception as e:
        await db.rollback()  # Rollback the transaction in case of error
        logging.exception(
            f"Exception occurred for order number {validated_data.orderNumber}: {e}"
        )
        return ORJSONResponse(
            content={"detail": "Erreur inconnue lors du scan de la pièce."},
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )

async def batch_scan(
    driver_id: int,
    validated_data: ScannedBatch,
    db: AsyncSession,
) -> ORJSONResponse:
    try:
        # Prepare query to fetch the driver order
        stmt = (
            select(DriverOrder)
            .where(
                and_(
                    DriverOrder.order_number == validated_data.orderNumber,
                    DriverOrder.store == validated_data.store,
                    DriverOrder.is_delivered == 0,
                )
            )
            .limit(1)
        )
        result = await db.execute(stmt)
        driver_order = result.scalar_one_or_none()

        # Check if the driver order exists
        if driver_order is None:
            logging.warning(
                f"Driver order not found for order number: {validated_data.orderNumber}"
            )
            return ORJSONResponse(
                content={"detail": "Commande non trouvée."},
                status_code=status.HTTP_404_NOT_FOUND,
            )

        order_info = orjson.loads(driver_order.order_info)

        for item in order_info:
            if item["item"] == validated_data.partNumber:
                # Mark all units of the item as scanned and confirmed scanned
                item["num_scanned"] = item["units"]
                item["confirmed_scanned"] = item["units"]

                driver_order.order_info = orjson.dumps(order_info).decode("utf-8")

                # Record the skipped part
                skipped_part = SkippedPart(
                    driver_id=driver_id,
                    order_number=validated_data.orderNumber,
                    store=validated_data.store,
                    part_number=validated_data.partNumber,
                    created_at=datetime.utcnow(),
                )

                db.add(skipped_part)  # Add the skipped part record to the database
                await db.commit()  # Commit the transaction to save changes
                await db.refresh(driver_order)  # Refresh the driver order instance

                return ORJSONResponse(
                    content={"num_scanned": item["num_scanned"]},
                    status_code=status.HTTP_200_OK,
                )

        logging.warning(
            f"Part number {validated_data.partNumber} not found in order number: {validated_data.orderNumber}"
        )
        return ORJSONResponse(
            content={"detail": "Pièce non trouvée dans la commande."},
            status_code=status.HTTP_404_NOT_FOUND,
        )
    except Exception as e:
        await db.rollback()  # Rollback the transaction in case of error
        logging.exception(
            f"Exception occurred for order number {validated_data.orderNumber}: {e}"
        )
        return ORJSONResponse(
            content={"detail": "Erreur inconnue lors du scan de la pièce."},
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )

async def received_by(data: DriverOrderModel.ReceivedBy, db: AsyncSession):
    async with db.begin():
        # Fetch the order by orderNumber
        result = await db.execute(select(DriverOrder).where(DriverOrder.order_number == data.order_number))
        driver_order = result.scalars().first()

        # Check if the order exists
        if not driver_order:
            raise HTTPException(status_code=404, detail="Order not found")

        # Update the received_by field
        driver_order.received_by = data.received_by

        # Commit the changes to save the updated received_by field
        await db.commit()

    return {"status": "success", "message": "Received by updated successfully"}
async def set_driver_order(
    driver_id: int,
    validated_data: InvoiceCode,
    db: AsyncSession,
    route: str,
    received_by: str
) -> ORJSONResponse:

    try:
        validated_data.set_client_number()
        client_number = validated_data.client_number

        if client_number is None:
            logging.error("Client number is missing. Cannot proceed with order processing.")
            return ORJSONResponse(
                content={"detail": "Client number is missing."},
                status_code=status.HTTP_400_BAD_REQUEST,
            )

        # Fetch the order from the secondary database
        order_query = (
            select(Order)
            .where(
                and_(
                    Order.order_number == validated_data.order_number,
                    Order.store == validated_data.store,
                )
            )
            .limit(1)
        )
        order_result = await db.execute(order_query)
        order = order_result.scalar_one_or_none()

        if order is None:
            return ORJSONResponse(
                content={"detail": "Commandes non trouvées."},
                status_code=status.HTTP_404_NOT_FOUND,
            )
        
        price = order.price
        job = order.job

        if not order.order_info:
            return ORJSONResponse(
                content={"detail": "Il n'y a pas de produits dans la commande."},
                status_code=status.HTTP_404_NOT_FOUND,
            )

        # Check if the order has already been processed
        existing_order_query = (
            select(DriverOrder)
            .where(
                and_(
                    DriverOrder.order_number == validated_data.order_number,
                    DriverOrder.store == validated_data.store,
                )
            )
            .limit(1)
        )
        existing_order_result = await db.execute(existing_order_query)
        existing_order = existing_order_result.scalar_one_or_none()

        if existing_order:
            return ORJSONResponse(
                content={"detail": "La commande a déjà été traitée."},
                status_code=status.HTTP_400_BAD_REQUEST,
            )
        
        # Parse and initialize num_scanned and confirmed_scanned fields
        try:
            order_info_data = json.loads(order.order_info)
            for item in order_info_data:
                # Initialize as integers if they aren't present
                if 'num_scanned' not in item:
                    item['num_scanned'] = 0
                if 'confirmed_scanned' not in item:
                    item['confirmed_scanned'] = 0
            initialized_order_info_json = json.dumps(order_info_data)
        except json.JSONDecodeError:
            return ORJSONResponse(
                content={"detail": "Invalid order_info data format."},
                status_code=status.HTTP_400_BAD_REQUEST,
            )

        # Merge order info with existing undelivered orders
        merged_order_info_json, _ = await merge_orders_for_client(
            driver_id, validated_data.client_number, initialized_order_info_json, validated_data.order_number, db
        )
        # Fetch driver username
        driver_username_query = (
            select(Driver.username)
            .where(Driver.id == driver_id)
            .limit(1)
        )
        driver_username_result = await db.execute(driver_username_query)
        driver_username = driver_username_result.scalar_one_or_none()

        if not driver_username:
            return ORJSONResponse(
                content={"detail": "Livreur non trouvé."},
                status_code=status.HTTP_404_NOT_FOUND,
            )

        # Check if the order exists in tentative_orders
        tentative_order_query = (
            select(DriverTentativeOrder)
            .where(
                and_(
                    DriverTentativeOrder.order_number == order.order_number,
                    DriverTentativeOrder.store == order.store,
                )
            )
            .limit(1)
        )

        tentative_order_result = await db.execute(tentative_order_query)
        tentative_order = tentative_order_result.scalar_one_or_none()

        if tentative_order:
            # Delete the order from tentative_orders
            delete_query = delete(DriverTentativeOrder).where(
                and_(
                    DriverTentativeOrder.order_number == order.order_number,
                    DriverTentativeOrder.store == order.store,
                )
            )
            await db.execute(delete_query)

            # Prepare data for new order in drivers_orders
            new_order_data = tentative_order
            address = tentative_order.address
            latitude = tentative_order.latitude
            longitude = tentative_order.longitude
        else:
            # Use fetch_clean_addresses to get latitude and longitude
            try:
                pos_arc_record = await fetch_pos_arc_record(
                    db, customer_number=validated_data.client_number, job=job
                )

                if pos_arc_record:
                    # Use existing coordinates if available
                    address = pos_arc_record.address
                    latitude = pos_arc_record.latitude
                    longitude = pos_arc_record.longitude
                else:
                    # Fallback: Use fetch_clean_addresses to generate new coordinates
                    address1, address2, address3 = order.address1, order.address2, order.address3
                    ship_addr1, ship_addr2, ship_addr3 = (
                        order.ship_addr1,
                        order.ship_addr2,
                        order.ship_addr3,
                    )
                    clean_address, latitude, longitude = await fetch_clean_addresses(
                        address1, address2, address3, ship_addr1, ship_addr2, ship_addr3
                    )

                    # Insert the new record into pos_arc_d_head
                    await insert_pos_arc_record(
                        db,
                        customer=order.client_name or "Unknown",
                        customer_number=validated_data.client_number or "Unknown",
                        job=job,
                        address=clean_address,
                        latitude=latitude,
                        longitude=longitude,
                    )

                    # Use the newly generated coordinates
                    address = clean_address

            except Exception as e:
                logging.error("Error occurred while fetching/generating coordinates:")
                logging.error(f"Error type: {type(e).__name__}")
                logging.error(f"Error message: {str(e)}")
                logging.error(f"Full traceback: {traceback.format_exc()}")
                return ORJSONResponse(
                    content={"detail": f"Failed to fetch or generate coordinates: {str(e)}"},
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                )

            if latitude is None or longitude is None:
                return ORJSONResponse(
                    content={"detail": "Unable to fetch valid coordinates."},
                    status_code=status.HTTP_400_BAD_REQUEST,
                )

        # Get the current time in Eastern Time
        created_at = datetime.now(ZoneInfo("America/New_York"))

        # Check if there’s an existing merged order for this driver
        existing_merged_order_query = (
            select(DriverOrder)
            .where(
                and_(
                    DriverOrder.order_number == validated_data.order_number,
                    DriverOrder.store == order.store,
                    DriverOrder.is_delivered == False
                )
            )
        )
        existing_merged_order_result = await db.execute(existing_merged_order_query)
        existing_merged_order = existing_merged_order_result.scalar_one_or_none()

        if existing_merged_order:
            # Update the existing merged order
            existing_merged_order.order_info = merged_order_info_json
            existing_merged_order.updated_at = created_at
            await db.commit()
        else:
            # Fetch the highest order_index for the given route and driver
            max_order_index_query = (
                select(func.coalesce(func.max(DriverOrder.order_index), 0))
                .where(
                    and_(
                        DriverOrder.route == route,
                        DriverOrder.driver_id == driver_id
                    )
                )
            )
            max_order_index_result = await db.execute(max_order_index_query)
            max_order_index = max_order_index_result.scalar_one() or 0

            # Increment order_index by 1 for the new order
            new_order_index = max_order_index + 1
            tracking_number = str(uuid.uuid4())[:8]

            # Initialize merged_order_numbers with the current order_number
            merged_orders = [validated_data.order_number]

            # Fetch any previous undelivered orders for the same driver, customer, and store
            existing_orders_query = (
                select(DriverOrder)
                .where(
                    and_(
                        DriverOrder.customer == validated_data.client_number,
                        DriverOrder.driver_id == driver_id,
                        DriverOrder.store == validated_data.store,
                        DriverOrder.is_delivered == False
                    )
                )
            )
            existing_orders_result = await db.execute(existing_orders_query)
            existing_orders = existing_orders_result.scalars().all()

            # Append previous order_numbers to merged_orders
            for existing_order in existing_orders:
                previous_orders = json.loads(existing_order.merged_order_numbers or "[]")
                for order_number in previous_orders:
                    if order_number not in merged_orders:
                        merged_orders.append(order_number)

            # Create a new merged order
            new_driver_order = DriverOrder(
                driver_id=driver_id,
                driver_name=driver_username,
                store=order.store,
                tracking_number=tracking_number,
                order_number=validated_data.order_number,
                merged_order_numbers=json.dumps(merged_orders),  # Save the complete list of order numbers
                customer=validated_data.client_number,
                order_info=merged_order_info_json,
                client_name=order.client_name,
                phone_number=order.phone_number,
                latitude=latitude,
                longitude=longitude,
                address=address,
                ship_addr=address,
                created_at=created_at,
                route=route,
                route_started=False,
                received_by=received_by,
                price=price,
                job=job,
                order_index=new_order_index
            )
            db.add(new_driver_order)
            await db.commit()

        # Remove the original undelivered orders for the same client, except the merged one
        delete_query = (
            delete(DriverOrder)
            .where(
                and_(
                    DriverOrder.customer == validated_data.client_number,
                    DriverOrder.driver_id == driver_id,
                    DriverOrder.is_delivered == False,
                    DriverOrder.order_number != validated_data.order_number
                )
            )
        )
        await db.execute(delete_query)
        await db.commit()

        return ORJSONResponse(
            content={"detail": "La commande a été associée au livreur."},
            status_code=status.HTTP_200_OK,
        )

    except ValueError as ve:
        logging.error(f"Validation error: {str(ve)}")
        await db.rollback()
        return ORJSONResponse(
            content={"detail": "Erreur de validation."},
            status_code=status.HTTP_400_BAD_REQUEST,
        )
    except Exception as e:
        logging.error(f"Unexpected error occurred: {str(e)}")
        await db.rollback()
        return ORJSONResponse(
            content={"detail": "Une erreur inconnue est survenue. Scanné 2 fois!"},
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
    
async def fetch_pos_arc_record(
    db: AsyncSession,  # This should be the primary database
    customer_number: str = None,
    client_name: str = None,
    job: int = None
):
    if job is None:
        logging.info(f"No job provided for customer {customer_number or client_name}, using default job='0'")
        job = 0

    try:
        if customer_number:
            query = text(
                "SELECT * FROM pos_arc_d_head WHERE customer_number = :customer_number AND job = :job LIMIT 1"
            )
            result = await db.execute(query, {"customer_number": customer_number, "job": job})
            return result.fetchone()
        elif client_name:
            query = text(
                "SELECT * FROM pos_arc_d_head WHERE customer = :client_name AND job = :job LIMIT 1"
            )
            result = await db.execute(query, {"client_name": client_name, "job": job})
            return result.fetchone()

    except Exception as e:
        print(f"Error in fetch_pos_arc_record: {str(e)}")
        return None

    raise ValueError("Either customer_number or client_name must be provided to fetch a POS ARC record.")

async def insert_pos_arc_record(
    db: AsyncSession,
    customer: str,
    customer_number: str,
    address: str,
    latitude: float,
    longitude: float,
    job: int,
):
    try:
        insert_query = text(
            """
            INSERT INTO pos_arc_d_head (customer, customer_number, address, latitude, longitude, job)
            VALUES (:customer, :customer_number, :address, :latitude, :longitude, :job)
            """
        )
        await db.execute(
            insert_query,
            {
                "customer": customer,
                "customer_number": customer_number,
                "address": address,
                "latitude": latitude,
                "longitude": longitude,
                "job": job,
            },
        )
        await db.commit()
    except Exception as e:
        # Log or print the error
        print(f"500 Internal Server Error: Failed to insert record into pos_arc_d_head - {str(e)}")
        # Optionally re-raise the exception if needed
        raise


async def fetch_or_create_coordinates(
    order_number: str, 
    job: int, 
    db_primary: AsyncSession,  
    db_secondary: AsyncSession
):
    try:
        # First, query full_orders using the secondary (warehouse) database
        query = text("""
            SELECT 
                order_number,
                customer,
                client_name,
                address1,
                address2,
                address3,
                ship_addr1,
                ship_addr2,
                ship_addr3,
                job
            FROM full_orders 
            WHERE order_number = :order_number 
            AND (job = :job OR (:job = 0 AND job IS NULL))
        """)
        
        result = await db_secondary.execute(query, {"order_number": order_number, "job": job})
        order_row = result.fetchone()

        if not order_row:
            # Try without job constraint
            query = text("""
                SELECT * FROM full_orders 
                WHERE order_number = :order_number 
                LIMIT 1
            """)
            result = await db_secondary.execute(query, {"order_number": order_number})
            order_row = result.fetchone()

        if not order_row:
            raise HTTPException(
                status_code=404,
                detail=f"Order not found: {order_number}"
            )

        # Extract customer information
        customer_number = order_row.customer
        client_name = order_row.client_name

        # Check for existing coordinates in pos_arc_d_head using the primary database
        pos_arc_record = await fetch_pos_arc_record(
            db_primary,  # Use primary database for pos_arc_d_head
            customer_number=customer_number,
            client_name=client_name,
            job=order_row.job
        )

        if pos_arc_record and pos_arc_record.latitude and pos_arc_record.longitude:
            return {
                "latitude": pos_arc_record.latitude,
                "longitude": pos_arc_record.longitude,
                "address": pos_arc_record.address
            }

        # Generate new coordinates if none exist
        clean_address, latitude, longitude = await fetch_clean_addresses(
            order_row.address1,
            order_row.address2,
            order_row.address3,
            order_row.ship_addr1,
            order_row.ship_addr2,
            order_row.ship_addr3
        )

        # Store new coordinates in pos_arc_d_head using primary database
        await insert_pos_arc_record(
            db_primary,  # Use primary database for pos_arc_d_head
            customer=client_name or "Unknown",
            customer_number=customer_number or "Unknown",
            address=clean_address,
            latitude=latitude,
            longitude=longitude,
            job=order_row.job
        )

        return {
            "latitude": latitude,
            "longitude": longitude,
            "address": clean_address
        }

    except Exception as e:
        print(f"Error in fetch_or_create_coordinates: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error processing order coordinates: {str(e)}"
        )