from itertools import groupby
import json
import logging
import string
from fastapi.responses import ORJSONResponse
from pyrate_limiter import Dict, Optional
import pytz
from sqlalchemy import and_, asc, case, desc, distinct, func, inspect, join, or_, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession
from model.posArcModel import PosArcDHead, UpdateClientRequest
from model.superDeliver import DriverOrderModel
from fastapi import Depends, HTTPException
from datetime import date, datetime, timedelta
from typing import List
from collections import defaultdict
from model.superDeliver.OrderModel import Order
from model.superDeliver.DriverModel import StoreId
from model.superDeliver.DriverOrderModel import DriverOrder, DriverStats, TimeRangeFilteredStats, TimeRangeStats, current_time_eastern
from model.superLocator.InvModel import InvLocations, Localisation
from starlette import status

from model.superLocator.ItemModel import Item, ItemReturn, Items, ItemsArchived, ItemsInfo, ItemsMissing, ItemsReturns
from model.superLocator.LoginModel import UsersLocator
from utility.util import STATE_OF_DEPLOYMENT, get_primary_db

def get_eastern_time():
    eastern = pytz.timezone('US/Eastern')
    return datetime.now(eastern)

async def get_client_orders_info(data: DriverOrderModel.ClientOrders, db_primary: AsyncSession, db_secondary: AsyncSession) -> List[dict]:
    try:
        not_delivered_datetime = datetime(1000, 1, 1, 12, 0, 0)

        # First: Query to fetch from primary (DriverOrderModel)
        query_primary = select(
            DriverOrderModel.DriverOrder.order_number,
            DriverOrderModel.DriverOrder.customer,
            DriverOrderModel.DriverOrder.client_name,
            DriverOrderModel.DriverOrder.order_info,
            DriverOrderModel.DriverOrder.delivered_at,
            DriverOrderModel.DriverOrder.driver_name,
            DriverOrderModel.DriverOrder.received_by
        ).where(
            DriverOrderModel.DriverOrder.store == data.store,
            DriverOrderModel.DriverOrder.delivered_at != not_delivered_datetime
        )

        # Apply search filter if provided
        if data.search:
            search_query = str(data.search)
            if search_query.isdigit():
                query_primary = query_primary.where(
                    DriverOrderModel.DriverOrder.customer == search_query
                )
            else:
                query_primary = query_primary.where(
                    DriverOrderModel.DriverOrder.client_name.ilike(f"%{search_query}%")
                )

        # Execute the first query on the primary database
        result_primary = await db_primary.execute(query_primary)
        primary_orders = result_primary.fetchall()

        if not primary_orders:
            return []  # Return early if no orders found

        # Get order numbers to query secondary database
        order_numbers = [order.order_number for order in primary_orders]

        # Second: Query to fetch created_at from secondary (Order) table using the order numbers
        query_secondary = select(
            Order.order_number,
            Order.created_at
        ).where(
            Order.store == data.store,
            Order.order_number.in_(order_numbers)  # Fetch only the required orders
        )

        # Execute the second query on the secondary database
        result_secondary = await db_secondary.execute(query_secondary)
        secondary_orders = {row.order_number: row.created_at for row in result_secondary.fetchall()}

        grouped_orders = defaultdict(lambda: {
            '1-20min': {
                'orders': [],  # List of orders in this range
                'count': 0  # Count of orders in this range
            },
            '21-40min': {
                'orders': [],
                'count': 0
            },
            '41-60min': {
                'orders': [],
                'count': 0
            },
            '60-90min': {
                'orders': [], 
                'count': 0
            },
            '90+min': {
                'orders': [],
                'count': 0
            },
            'avg_delivery_time': 0,  # Add this field to store average delivery time in minutes
            'valid_delivery_time': 0,  # Sum of delivery times under 2 hours
            'valid_order_count': 0,  # Count of valid orders with delivery time under 2 hours
            'total_orders': 0  # Add this field to count total orders per customer
        })

        for order in primary_orders:
            created_at = secondary_orders.get(order.order_number)  # Get created_at from secondary data

            if not created_at:
                continue  # Skip if no matching order in secondary

            # Parse order_info
            try:
                order_info_json = json.loads(order.order_info)
            except json.JSONDecodeError:
                order_info_json = order.order_info

            # Calculate the delivery time in minutes
            delivery_time_minutes = round((order.delivered_at - created_at).total_seconds() / 60)

            # Convert delivery time to hours and minutes
            if delivery_time_minutes >= 60:
                delivery_hours = delivery_time_minutes // 60
                delivery_minutes = delivery_time_minutes % 60
                delivery_time_str = f"{delivery_hours} hours {delivery_minutes} minutes" if delivery_minutes > 0 else f"{delivery_hours} hours"
            else:
                delivery_time_str = f"{delivery_time_minutes} minutes"

            # Determine the delivery time category
            if delivery_time_minutes <= 20:
                time_group = '1-20min'
            elif 21 <= delivery_time_minutes <= 40:
                time_group = '21-40min'
            elif 41 <= delivery_time_minutes <= 60:
                time_group = '41-60min'
            elif 61 <= delivery_time_minutes <= 90:
                time_group = '60-90min'
            else:
                time_group = '90+min'  

            grouped_orders[(order.client_name, order.customer)][time_group]['orders'].append({
                "order_number": order.order_number,
                "order_info": order_info_json,
                "created_at": created_at,
                "delivered_at": order.delivered_at,
                "driver_name": order.driver_name,
                "received_by": order.received_by,
                "delivery_time": delivery_time_str  # Store the delivery time as "X hours Y minutes"
            })
            grouped_orders[(order.client_name, order.customer)][time_group]['count'] += 1  # Increment count for this time group

            # Add delivery time in minutes for averaging later, only if it's less than or equal to 120 minutes (2 hours)
            if delivery_time_minutes <= 120:
                grouped_orders[(order.client_name, order.customer)]['valid_delivery_time'] += delivery_time_minutes
                grouped_orders[(order.client_name, order.customer)]['valid_order_count'] += 1

            # Increment the total orders count for the customer
            grouped_orders[(order.client_name, order.customer)]['total_orders'] += 1

        # Calculate the average delivery time for each client and update the result
        for (client_name, customer), orders_data in grouped_orders.items():
            valid_order_count = grouped_orders[(client_name, customer)]['valid_order_count']
            if valid_order_count > 0:
                avg_delivery_minutes = grouped_orders[(client_name, customer)]['valid_delivery_time'] // valid_order_count
                if avg_delivery_minutes >= 60:
                    avg_delivery_hours = avg_delivery_minutes // 60
                    avg_delivery_remaining_minutes = avg_delivery_minutes % 60
                    grouped_orders[(client_name, customer)]['avg_delivery_time'] = f"{avg_delivery_hours} hours {avg_delivery_remaining_minutes} minutes" if avg_delivery_remaining_minutes > 0 else f"{avg_delivery_hours} hours"
                else:
                    grouped_orders[(client_name, customer)]['avg_delivery_time'] = f"{avg_delivery_minutes} minutes"
            else:
                grouped_orders[(client_name, customer)]['avg_delivery_time'] = "No valid orders"

        # Format the result as a list of dictionaries for each client_name
        client_orders = [
            {
                "client_name": client_name,
                "customer": customer,
                "orders_by_time": grouped_orders[(client_name, customer)],
                "avg_delivery_time": grouped_orders[(client_name, customer)]['avg_delivery_time'],  # Include average delivery time
                "total_orders": grouped_orders[(client_name, customer)]['total_orders']  # Include total order count
            }
            for (client_name, customer) in grouped_orders.keys()
        ]

        return client_orders

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

async def get_all_localisation(
    validated_data: Localisation,
    db_primary: AsyncSession,
    limit: int,
    offset: int,
    search_term: Optional[str] = None
) -> ORJSONResponse:
    try:
        # Base query
        query = select(
            InvLocations.upc,
            InvLocations.name, 
            func.max(InvLocations.level).label("level"),
            func.max(InvLocations.row).label("row"),
            func.max(InvLocations.side).label("side"),
            func.max(InvLocations.column).label("column"),
            func.max(InvLocations.shelf).label("shelf"),
            func.max(InvLocations.bin).label("bin"),
            func.max(InvLocations.full_location).label("full_location"), 
            func.max(InvLocations.updated_by).label("updated_by"),
            func.max(InvLocations.updated_at).label("updated_at"),
            func.max(InvLocations.created_at).label("created_at"),
            func.max(InvLocations.created_by).label("created_by"),
            func.count(InvLocations.id).label("item_count")
        ).where((InvLocations.store == validated_data.storeId) &
                (InvLocations.is_archived == False) &
                (InvLocations.name.not_like("%inconnu%"))
        )

        # Apply search term filter if provided
        if search_term:
            search_filter = InvLocations.name.ilike(f"%{search_term}%")
            query = query.where(search_filter)

        # Apply grouping, limit, and offset
        query = query.group_by(InvLocations.upc, InvLocations.full_location).limit(limit).offset(offset)
        result = await db_primary.execute(query)
        locations = result.all()

        # Get the total count with the search filter applied
        count_query = select(func.count()).where(
            (InvLocations.store == validated_data.storeId) &
            (InvLocations.is_archived == False) &
            (InvLocations.name.not_like("%inconnu%"))
        )
        if search_term:
            count_query = count_query.where(search_filter)

        count_result = await db_primary.execute(count_query)
        total_count = count_result.scalar()

        location_dicts = [
            {
                "upc": location.upc,
                "name": location.name,
                "level": location.level,
                "row": location.row,
                "side": location.side,
                "column": location.column,
                "shelf": location.shelf,
                "bin": location.bin,
                "full_location": location.full_location,
                "updated_by": location.updated_by,
                "updated_at": location.updated_at,
                "created_at": location.created_at,
                "created_by": location.created_by,
                "item_count": location.item_count
            } 
            for location in locations
        ]

        return ORJSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "localisations": location_dicts,
                "total_count": total_count
            },
            headers={"X-Total-Count": str(total_count)}
        )

    except Exception as e:
        logging.error(f"Error fetching localisations: {e}", exc_info=True)
        return ORJSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": "Unknown error occurred while retrieving localisations."},
        )
    
async def archive_item_localisation(upc: str, full_location: str, db: AsyncSession) -> dict:
    try:
        # Fetch the first localisation by upc and full_location that is not archived
        query = select(InvLocations).where(
            InvLocations.upc == upc,
            InvLocations.full_location == full_location,
            InvLocations.is_archived == False
        ).limit(1)  # Limit to only one row
        result = await db.execute(query)
        localisation = result.scalar_one_or_none()  # Retrieve only one matching row

        if not localisation:
            raise HTTPException(status_code=404, detail="Localization not found")

        # Archive the localisation by setting is_archived=True
        stmt = update(InvLocations).where(
            InvLocations.id == localisation.id
        ).values(is_archived=True)
        await db.execute(stmt)
        await db.commit()

        return {"detail": "Localization archived successfully"}

    except Exception as e:
        logging.error(f"Error archiving localization: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to archive localization")
    
async def get_all_orders_by_store(validated_data: DriverOrderModel.AllOrdersByStore, db_primary: AsyncSession):
        try:
            # Query to get all orders filtered by storeId
            stmt = select(DriverOrderModel.DriverOrder).where(
                DriverOrderModel.DriverOrder.store == validated_data.storeId
            )
            
            result = await db_primary.execute(stmt)
            orders = result.scalars().all()

            # Function to convert model instance to dictionary
            def model_to_dict(model_instance):
                return {c.key: getattr(model_instance, c.key) for c in inspect(model_instance).mapper.column_attrs}

            # If no orders found, return an appropriate message
            if not orders:
                return ORJSONResponse(status_code=404, content={"message": "No orders found for this storeId"})
            
            # Convert each order to a dictionary and return as JSON response
            return ORJSONResponse(status_code=200, content={"orders": [model_to_dict(order) for order in orders]})
        
        except Exception as e:
            # Catch any exception and return an error response
            return ORJSONResponse(status_code=500, content={"message": f"Error retrieving orders: {str(e)}"})
        
async def search_driver_order(
    search_query: str,
    store: str,
    db_primary: AsyncSession,
    db_secondary: AsyncSession
) -> Dict[str, Dict[str, List[Dict[str, any]]]]:
    # Define BASE_URL for image paths based on deployment state
    BASE_URL = {
        "prod": "https://deliver-prod.pasuper.xyz",
        "dev": "https://deliver-dev.pasuper.xyz",
        "local": "http://127.0.0.1:8000",
    }
    image_url_env = BASE_URL.get(STATE_OF_DEPLOYMENT, BASE_URL["local"])
    
    # Create an exact match search pattern
    search_pattern = f"%{search_query}%"
    
    # Construct the search query with all relevant fields
    query = (
        select(DriverOrder)
        .where(
            and_(  # Add AND condition to combine store filter with search
                DriverOrder.store == store,  # Add store filter
                or_(
                    func.lower(DriverOrder.order_number).like(func.lower(search_pattern)),
                    func.lower(DriverOrder.tracking_number).like(func.lower(search_pattern)),
                    func.lower(DriverOrder.customer).like(func.lower(search_pattern)),
                    func.lower(DriverOrder.client_name).like(func.lower(search_pattern)),
                    func.lower(DriverOrder.merged_order_numbers).like(func.lower(search_pattern))
                )
            )
        )
    )

    # Execute the query
    result = await db_primary.execute(query)
    driver_orders = result.scalars().all()

    # If we found the order, get its creation time from secondary database
    if driver_orders:
        order_numbers = [order.order_number for order in driver_orders]
        secondary_query = (
            select(Order.order_number, Order.created_at)
            .where(Order.order_number.in_(order_numbers))
        )
        secondary_result = await db_secondary.execute(secondary_query)
        secondary_data = {row.order_number: row.created_at for row in secondary_result.fetchall()}
    else:
        secondary_data = {}

    # Group orders by route for consistent response format
    grouped_orders = {}
    for route, orders in groupby(driver_orders, key=lambda x: x.route):
        orders_list = list(orders)
        driver_name = orders_list[0].driver_name if orders_list else "Unknown"

        grouped_orders[route] = {
            "driver_name": driver_name,
            "orders": []
        }

        for order in orders_list:
            # Parse order_info JSON
            try:
                order_items = json.loads(order.order_info)
            except json.JSONDecodeError:
                order_items = []

            # Format photo URL
            photo_url = None
            if order.photo_filename:
                # Clean up photo path
                for path in ["/home/images-dev", "/home/images", "home/images-dev", "home/images"]:
                    order.photo_filename = order.photo_filename.replace(path, "images")
                photo_url = f"{image_url_env}/{order.photo_filename}"

            # Get full order creation time from secondary database
            full_order_created_at = secondary_data.get(order.order_number)

            # Build comprehensive order information
            grouped_orders[route]["orders"].append({
                "order_number": order.order_number,
                "merged_order_numbers": order.merged_order_numbers,
                "tracking_number": order.tracking_number,
                "customer": order.customer,
                "client_name": order.client_name,
                "phone_number": order.phone_number,
                "address": order.address,
                "ship_addr": order.ship_addr,
                "latitude": order.latitude,
                "longitude": order.longitude,
                "created_at": order.created_at,
                "full_order_created_at": full_order_created_at,
                "arrived_at": order.arrived_at,
                "delivered_at": order.delivered_at,
                "is_arrived": order.is_arrived,
                "is_delivered": order.is_delivered,
                "items": order_items,
                "order_index": order.order_index,
                "photo_filename": photo_url,
                "received_by": order.received_by,
                "price": order.price,
                "route_started": order.route_started,
                "job": order.job
            })

    return grouped_orders    


async def get_all_drivers_orders(
    validated_data: StoreId, 
    db_primary: AsyncSession, 
    db_secondary: AsyncSession, 
    offset: int = 0, 
    limit: int = 100, 
    search: List[str] = None
) -> Dict[str, Dict[str, List[Dict[str, any]]]]:
    # Define BASE_URL for image paths based on deployment state
    BASE_URL = {
        "prod": "https://deliver-prod.pasuper.xyz",
        "dev": "https://deliver-dev.pasuper.xyz",
        "local": "http://127.0.0.1:8000",
    }
    image_url_env = BASE_URL.get(STATE_OF_DEPLOYMENT, BASE_URL["local"])
    
    # Build the base query
    query = (
        select(DriverOrder)
        .where(DriverOrder.store == validated_data.store)
        .order_by(desc(DriverOrder.created_at))
        .offset(offset)
        .limit(limit)
    )

    # Handle array-based search terms
    if search and isinstance(search, list) and len(search) > 0:
        search_conditions = []
        for search_term in search:
            if search_term:  # Only add non-empty search terms
                term_condition = or_(
                    DriverOrder.tracking_number.like(f"%{search_term}%"),
                    DriverOrder.order_number.like(f"%{search_term}%"),
                    DriverOrder.customer.like(f"%{search_term}%"),
                    DriverOrder.client_name.like(f"%{search_term}%"),
                    DriverOrder.merged_order_numbers.like(f"%{search_term}%")
                )
                search_conditions.append(term_condition)
        
        # Add search conditions to query if any exist
        if search_conditions:
            query = query.where(or_(*search_conditions))

    # Execute the primary query
    result = await db_primary.execute(query)
    driver_orders = result.scalars().all()

    # Fetch related data from secondary database
    order_numbers = [order.order_number for order in driver_orders]
    secondary_query = (
        select(Order.order_number, Order.created_at)
        .where(Order.order_number.in_(order_numbers))
    )
    secondary_result = await db_secondary.execute(secondary_query)
    secondary_data = {row.order_number: row.created_at for row in secondary_result.fetchall()}

    # Group orders by route and parse order_info
    grouped_orders = {}
    for route, orders in groupby(driver_orders, key=lambda x: x.route):
        orders_list = list(orders)
        driver_name = orders_list[0].driver_name if orders_list else "Unknown"

        grouped_orders[route] = {
            "driver_name": driver_name,
            "orders": []
        }

        for order in orders_list:
            # Parse order_info field safely
            try:
                order_items = json.loads(order.order_info)
            except json.JSONDecodeError:
                order_items = []

            # Format the photo filename URL
            photo_url = None
            if order.photo_filename:
                for path in ["/home/images-dev", "/home/images", "home/images-dev", "home/images"]:
                    order.photo_filename = order.photo_filename.replace(path, "images")
                photo_url = f"{image_url_env}/{order.photo_filename}"

            # Get the full order creation date from secondary data
            full_order_created_at = secondary_data.get(order.order_number)

            # Build the order data structure
            grouped_orders[route]["orders"].append({
                "order_number": order.order_number,
                "merged_order_numbers": order.merged_order_numbers,
                "tracking_number": order.tracking_number,
                "client_name": order.client_name,
                "address": order.address,
                "latitude": order.latitude,
                "longitude": order.longitude,
                "created_at": order.created_at,
                "full_order_created_at": full_order_created_at,
                "arrived_at": order.arrived_at,
                "delivered_at": order.delivered_at,
                "items": order_items,
                "order_index": order.order_index,
                "photo_filename": photo_url
            })

    return grouped_orders

async def get_orders_count(validated_data: StoreId, db_primary: AsyncSession):
    try:
        # Query to get the total count of orders for the store
        total_orders_stmt = select(func.count(DriverOrderModel.DriverOrder.id)).where(
            DriverOrderModel.DriverOrder.store == validated_data.store
        )

        # Query to get all delivered orders' created_at and delivered_at fields
        delivered_orders_stmt = select(
            DriverOrderModel.DriverOrder.created_at,
            DriverOrderModel.DriverOrder.delivered_at
        ).where(
            DriverOrderModel.DriverOrder.store == validated_data.store,
            DriverOrderModel.DriverOrder.is_delivered == True
        )

        # Execute both queries
        total_orders_result = await db_primary.execute(total_orders_stmt)
        delivered_orders_result = await db_primary.execute(delivered_orders_stmt)

        # Get the total count of orders
        total_orders = total_orders_result.scalar()

        # Fetch all the delivered orders' timestamps
        delivered_orders = delivered_orders_result.all()

        # Calculate the total delivery time for all delivered orders
        total_delivery_time = 0
        delivered_count = 0

        for order in delivered_orders:
            created_at = order.created_at
            delivered_at = order.delivered_at

            # Ensure both timestamps are available and valid
            if created_at and delivered_at:
                # Calculate delivery time in seconds
                delivery_time_seconds = (delivered_at - created_at).total_seconds()
                total_delivery_time += delivery_time_seconds
                delivered_count += 1

        # Calculate the average delivery time (in seconds) if there are delivered orders
        avg_delivery_time_seconds = total_delivery_time / delivered_count if delivered_count > 0 else None

        # Convert average delivery time from seconds to hours and minutes
        if avg_delivery_time_seconds is not None:
            avg_delivery_hours = int(avg_delivery_time_seconds // 3600)
            avg_delivery_minutes = int((avg_delivery_time_seconds % 3600) // 60)
            avg_delivery_time = f"{avg_delivery_hours}h {avg_delivery_minutes}m"
        else:
            avg_delivery_time = None

        # Prepare the response data
        response_data = {
            "total_orders": total_orders,
            "delivered_orders_count": delivered_count,
            "average_delivery_time": avg_delivery_time,  # This is in hours and minutes
        }

        return response_data

    except Exception as e:
        # Handle exceptions and return an error message
        return {"message": f"Error retrieving orders: {str(e)}", "status_code": 500}
    
async def get_all_missing_items_v2(
    validated_data: StoreId,
    db: AsyncSession,
) -> ORJSONResponse:
    try:
        # Query the ItemsMissing table directly - no need to filter by is_missing since all items in this table are missing
        query = select(ItemsMissing).where(ItemsMissing.store == validated_data.store)
        result = await db.execute(query)
        missing_items = result.scalars().all()

        # Convert each item to the Item model and serialize to JSON
        # Note that the structure remains the same since ItemsMissing has identical fields to Items
        items_data = [
            Item(
                id=item.id,
                store=item.store,
                order_number=item.order_number,
                item=item.item,
                description=item.description,
                units=item.units,
                state=item.state,
                created_at=item.created_at,
                updated_at=item.updated_at,
                updated_by=item.updated_by,
                loc=item.loc,
                is_reserved=item.is_reserved,
                is_archived=item.is_archived,
                is_missing=True,  # Always true since it's from the missing items table
                upc=item.upc,
                picked_by=item.picked_by,
                reserved_by=item.reserved_by
            ).model_dump() for item in missing_items
        ]

        return ORJSONResponse(
            status_code=status.HTTP_200_OK,
            content=items_data
        )

    except Exception as e:
        logging.error(f"Error fetching missing items: {e}")
        return ORJSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": "Erreur lors de la récupération des articles manquants."}
        )
    
async def update_client_in_db(client_id: int, client_data: UpdateClientRequest, db: AsyncSession):
    # Fetch the client by ID using select and async session
    result = await db.execute(select(PosArcDHead).filter(PosArcDHead.id == client_id))
    client = result.scalars().first()

    if not client:
        raise HTTPException(status_code=404, detail="Client not found")

    # Update client fields
    client.customer = client_data.customer
    client.customer_number = client_data.customer_number
    client.job = client_data.job
    client.address = client_data.address
    client.postal_code = client_data.postal_code
    client.latitude = client_data.latitude
    client.longitude = client_data.longitude

    # Commit the changes asynchronously
    await db.commit()

    return client

async def delete_client_from_db(client_id: int, db: AsyncSession):
    # Fetch the client by ID using select and async session
    result = await db.execute(select(PosArcDHead).filter(PosArcDHead.id == client_id))
    client = result.scalars().first()

    if not client:
        raise HTTPException(status_code=404, detail="Client not found")

    # Delete the client
    await db.delete(client)
    await db.commit()

async def fetch_customers_with_psl(
    db: AsyncSession, 
    offset=0, 
    limit=20,
    start_date: Optional[str] = None, 
    end_date: Optional[str] = None, 
    order_number: Optional[str] = None,
    store: Optional[int] = None
):
    try:
        # Prepare base filters
        filters = []
        
        # Optimize date handling
        if start_date:
            start_datetime = datetime.strptime(f"{start_date} 00:00:00", "%Y-%m-%d %H:%M:%S")
            filters.append(Order.created_at >= start_datetime)

        if end_date:
            end_datetime = datetime.strptime(f"{end_date} 23:59:59", "%Y-%m-%d %H:%M:%S")
            filters.append(Order.created_at <= end_datetime)

        # Optimize order number search
        if order_number:
            filters.append(Order.order_number.ilike(f"{order_number}%"))  # Changed to prefix-only search

        if store:
            filters.append(Order.store == store)

        # Optimize PSL address search using a more efficient pattern
        psl_pattern = "%PSL%"
        psl_filter = or_(
            Order.address1.ilike(psl_pattern),
            Order.address2.ilike(psl_pattern),
            Order.address3.ilike(psl_pattern),
            Order.ship_addr1.ilike(psl_pattern),
            Order.ship_addr2.ilike(psl_pattern),
            Order.ship_addr3.ilike(psl_pattern)
        )
        filters.append(psl_filter)

        # Combined query with JOIN to avoid separate queries
        main_query = (
            select(
                Order.id,
                Order.customer,
                Order.client_name,
                Order.order_number,
                Order.created_at,
                Order.store,
                DriverOrder.driver_name
            )
            .join(
                DriverOrder,
                and_(
                    Order.order_number == DriverOrder.order_number,
                    DriverOrder.is_delivered == True
                ),
                isouter=False
            )
            .where(and_(*filters))
            .order_by(desc(Order.created_at))
            .with_hint(Order, 'FORCESEEK(IX_orders_created_at)', 'mssql')
        )

        # Execute count query first
        count_query = select(func.count()).select_from(main_query.subquery())
        total_count = await db.scalar(count_query)

        # Then execute main query with pagination
        paginated_query = main_query.offset(offset).limit(limit)
        result = await db.execute(paginated_query)
        customers_data = result.fetchall()

        # Format results efficiently
        customers_list = [
            {
                "customer": row.customer,
                "client_name": row.client_name,
                "order_number": row.order_number,
                "created_date": row.created_at,
                "store": row.store,
                "driver_name": row.driver_name
            }
            for row in customers_data
        ]

        return customers_list, total_count

    except Exception as e:
        print(f"Error executing query: {e}")
        raise

async def form_order_info(db: AsyncSession, item_name: str, quantity: int, store_id: int):
    print(f"Starting process for item: {item_name} with quantity: {quantity} and store: {store_id}")
    
    # Determine the prefix based on store_id (e.g., "01" for store 1, "02" for store 2, etc.)
    store_prefix = f"{store_id:02d}"  # Formats as "01", "02", "03", etc.

    # Query the locations table to find all unarchived locations for the item
    result = await db.execute(
        select(InvLocations)
        .filter(InvLocations.name == item_name)
        .filter(InvLocations.full_location.startswith(store_prefix))  # Ensure the location starts with the correct store prefix
        .filter(InvLocations.is_archived == False)  # Check that the location is not archived
    )
    locations = result.scalars().all()

    # Count available locations
    available_count = len(locations)
    
    if available_count == 0:
        # No matching locations found
        print(f"No available locations for item {item_name} in store {store_id}")
        full_location = f"{store_prefix}0000000"
        formatted_upc = json.dumps(["000000000000"])  # Default UPC if not found
        quantity_remaining = quantity
    else:
        full_location = ""
        upcs = []
        quantity_remaining = quantity

        for location in locations:
            if quantity_remaining <= 0:
                break

            # Add location to the result
            full_location += f"{location.full_location},"
            upcs.append(location.upc)

            # Mark location as archived
            location.is_archived = True
            quantity_remaining -= 1

        # Commit changes to archive used locations
        await db.commit()

        # If there are no more locations and quantity is still remaining, fallback to default
        if quantity_remaining > 0:
            print(f"Insufficient stock for item {item_name} in store {store_id}")
            full_location += f"{store_prefix}0000000,"
            upcs.append("000000000000")

        formatted_upc = json.dumps(upcs)

    # Determine the order number based on store_id from the request
    order_number = f"F{str(store_id).rjust(2, '0')}"

    if not order_number:
        raise ValueError("Order number mapping not found for the provided store_id")

    # Create a new record for the order_info table
    new_item = Items(
        store=store_id,
        order_number=order_number,
        item=item_name,
        description="show room",
        units=quantity - quantity_remaining,  # Total units fulfilled
        loc=full_location.strip(","),  # Remove trailing comma
        upc=formatted_upc,  # Store upc as a JSON string to mimic an array
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
        state='0',  # default state as per your requirements
        is_archived=False,  # Ensure the item is not archived
    )

    # Add, commit, and refresh the new item to the order_info table
    db.add(new_item)
    await db.commit()
    await db.refresh(new_item)

    # Convert `new_item` to a dictionary for response
    return {
        column.name: getattr(new_item, column.name)
        for column in new_item.__table__.columns
    }


async def get_delivery_stats_info_by_store(store_id: int, db_primary: AsyncSession) -> Dict[str, TimeRangeStats]:
    try:
        # Create timezone-aware not_delivered_datetime
        eastern_tz = pytz.timezone('America/New_York')
        not_delivered_datetime = datetime(1000, 1, 1, 12, 0, 0).replace(tzinfo=eastern_tz)
        now = get_eastern_time()  # Assuming this returns an aware datetime

        # Optimize date range calculations
        date_ranges = {
            'last_30_days': now - timedelta(days=30),
            'last_60_days': now - timedelta(days=60),
            'last_90_days': now - timedelta(days=90),
            'last_120_days': now - timedelta(days=120)
        }

        # Combine queries using JOIN to reduce database roundtrips
        combined_query = select(
            DriverOrder.order_number,
            DriverOrder.delivered_at,
            DriverOrder.created_at.label('driver_created_at'),
            Order.created_at.label('order_created_at')
        ).join(
            Order,
            DriverOrder.order_number == Order.order_number,
            isouter=False
        ).where(
            and_(
                DriverOrder.store == store_id,
                DriverOrder.delivered_at != not_delivered_datetime,
                DriverOrder.delivered_at >= date_ranges['last_120_days']
            )
        ).with_hint(DriverOrder, 'FORCESEEK(IX_driver_orders_store_id)', 'mssql')

        # Execute single combined query
        result = await db_primary.execute(combined_query)
        orders = result.fetchall()

        if not orders:
            return {str(store_id): TimeRangeStats(
                min_1_20=0, min_21_40=0, min_41_60=0, min_60_90=0, min_90_plus=0,
                last_30_days=0, last_60_days=0, last_90_days=0, last_120_days=0,
                avg_picking_time=0
            )}

        stats = {
            'min_1_20': 0, 'min_21_40': 0, 'min_41_60': 0, 
            'min_60_90': 0, 'min_90_plus': 0,
            'last_30_days': 0, 'last_60_days': 0, 'last_90_days': 0, 'last_120_days': 0
        }
        
        total_picking_time = 0
        valid_picking_time_count = 0

        for order in orders:
            # Ensure timezone awareness for all datetime fields
            delivered_at = order.delivered_at.replace(tzinfo=eastern_tz) if order.delivered_at.tzinfo is None else order.delivered_at
            driver_created_at = order.driver_created_at.replace(tzinfo=eastern_tz) if order.driver_created_at.tzinfo is None else order.driver_created_at
            order_created_at = order.order_created_at.replace(tzinfo=eastern_tz) if order.order_created_at.tzinfo is None else order.order_created_at

            # Calculate times with timezone-aware datetimes
            picking_time_minutes = round((driver_created_at - order_created_at).total_seconds() / 60)
            if 0 <= picking_time_minutes <= 90:
                total_picking_time += picking_time_minutes
                valid_picking_time_count += 1

            delivery_time_minutes = round((delivered_at - order_created_at).total_seconds() / 60)
            
            if delivery_time_minutes <= 20:
                stats['min_1_20'] += 1
            elif delivery_time_minutes <= 40:
                stats['min_21_40'] += 1
            elif delivery_time_minutes <= 60:
                stats['min_41_60'] += 1
            elif delivery_time_minutes <= 90:
                stats['min_60_90'] += 1
            else:
                stats['min_90_plus'] += 1

            # Single pass date range check with timezone-aware comparison
            if delivered_at >= date_ranges['last_30_days']:
                stats['last_30_days'] += 1
                stats['last_60_days'] += 1
                stats['last_90_days'] += 1
                stats['last_120_days'] += 1
            elif delivered_at >= date_ranges['last_60_days']:
                stats['last_60_days'] += 1
                stats['last_90_days'] += 1
                stats['last_120_days'] += 1
            elif delivered_at >= date_ranges['last_90_days']:
                stats['last_90_days'] += 1
                stats['last_120_days'] += 1
            elif delivered_at >= date_ranges['last_120_days']:
                stats['last_120_days'] += 1

        avg_picking_time = round(total_picking_time / valid_picking_time_count) if valid_picking_time_count > 0 else 0

        return {str(store_id): TimeRangeStats(**stats, avg_picking_time=avg_picking_time)}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

async def get_delivery_counts_info_by_date_range(
    store_id: int, start_date: date, end_date: date, db_primary: AsyncSession, db_secondary: AsyncSession
) -> DriverOrderModel.DeliveryStatsResponse:
    try:
        start_datetime = datetime.combine(start_date, datetime.min.time())
        end_datetime = datetime.combine(end_date, datetime.max.time())

        query_primary = select(
            DriverOrder.order_number,
            DriverOrder.delivered_at,
            DriverOrder.created_at.label('driver_created_at'),
            DriverOrder.driver_name
        ).where(
            DriverOrder.store == store_id,
            DriverOrder.delivered_at.between(start_datetime, end_datetime),
            DriverOrder.is_delivered == True
        )

        result_primary = await db_primary.execute(query_primary)
        primary_orders = result_primary.fetchall()

        if not primary_orders:
            return {
                str(store_id): TimeRangeFilteredStats(
                    min_1_20=0,
                    min_21_40=0,
                    min_41_60=0,
                    min_60_90=0,    # New category
                    min_90_plus=0,  # Renamed category
                    avg_picking_time=0,
                    unique_drivers=0
                )
            }

        unique_drivers = set()
        order_numbers = [order.order_number for order in primary_orders]

        query_secondary = select(Order.order_number, Order.created_at).where(
            Order.order_number.in_(order_numbers)
        )

        result_secondary = await db_secondary.execute(query_secondary)
        secondary_orders = {row.order_number: row.created_at for row in result_secondary.fetchall()}

        # Initialize counters with new categories
        min_1_20, min_21_40, min_41_60, min_60_90, min_90_plus = 0, 0, 0, 0, 0
        total_picking_time = 0
        valid_picking_time_count = 0

        for order in primary_orders:
            if order.driver_name:
                unique_drivers.add(order.driver_name)
                
            delivered_at = order.delivered_at
            created_at = secondary_orders.get(order.order_number)
            if not created_at:
                continue

            picking_time_minutes = round((order.driver_created_at - created_at).total_seconds() / 60)
            if 0 <= picking_time_minutes <= 90:
                total_picking_time += picking_time_minutes
                valid_picking_time_count += 1

            delivery_time_minutes = round((delivered_at - created_at).total_seconds() / 60)

            # Updated time categorization
            if delivery_time_minutes <= 20:
                min_1_20 += 1
            elif 21 <= delivery_time_minutes <= 40:
                min_21_40 += 1
            elif 41 <= delivery_time_minutes <= 60:
                min_41_60 += 1
            elif 61 <= delivery_time_minutes <= 90:  # New category
                min_60_90 += 1
            else:  # Now represents 90+ minutes
                min_90_plus += 1

        avg_picking_time = round(total_picking_time / valid_picking_time_count) if valid_picking_time_count > 0 else 0

        return {
            str(store_id): TimeRangeFilteredStats(
                min_1_20=min_1_20,
                min_21_40=min_21_40,
                min_41_60=min_41_60,
                min_60_90=min_60_90,    # New category
                min_90_plus=min_90_plus, # Renamed category
                avg_picking_time=avg_picking_time,
                unique_drivers=len(unique_drivers)
            )
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
async def count_locations_by_user(db: AsyncSession, store: int):
    now = get_eastern_time()
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    last_30_days = now - timedelta(days=30)
    last_60_days = now - timedelta(days=60)

    # Subquery to union created and updated locations
    locations_union = (
        select(
            InvLocations.id,
            InvLocations.updated_at,
            InvLocations.created_by.label('user_id')
        )
        .where(InvLocations.store == store)
        .union_all(
            select(
                InvLocations.id,
                InvLocations.updated_at,
                InvLocations.updated_by.label('user_id')
            )
            .where(
                and_(
                    InvLocations.store == store,
                    InvLocations.updated_by.isnot(None)  # Only include if actually updated by someone
                )
            )
        )
        .alias('locations_union')
    )

    stmt = (
        select(
            UsersLocator.username,
            func.count(distinct(locations_union.c.id)).label("total_location_count"),
            func.count(distinct(
                case(
                    (locations_union.c.updated_at >= today_start, locations_union.c.id),
                    else_=None
                )
            )).label("today_count"),
            func.count(distinct(
                case(
                    (locations_union.c.updated_at >= last_30_days, locations_union.c.id),
                    else_=None
                )
            )).label("last_30_days_count"),
            func.count(distinct(
                case(
                    ((locations_union.c.updated_at >= last_60_days) & 
                     (locations_union.c.updated_at < last_30_days), locations_union.c.id),
                    else_=None
                )
            )).label("days_31_to_60_count")
        )
        .select_from(UsersLocator)
        .join(
            locations_union,
            UsersLocator.id == locations_union.c.user_id
        )
        .group_by(UsersLocator.username)
    )

    results = await db.execute(stmt)
    result_list = results.all()

    locations_count_by_user = {
        result.username: {
            "total_location_count": result.total_location_count,
            "today_count": result.today_count,
            "last_30_days_count": result.last_30_days_count,
            "days_31_to_60_count": result.days_31_to_60_count
        }
        for result in result_list
    }
    return locations_count_by_user

async def count_items_picked_by_user(db: AsyncSession, store: int):
    now = get_eastern_time()
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    last_30_days = now - timedelta(days=30)
    last_60_days = now - timedelta(days=60)

    stmt = (
        select(
            ItemsArchived.picked_by,
            func.count(ItemsArchived.id).label("total_pick_count"),
            func.sum(case(
                (ItemsArchived.updated_at >= today_start, 1),
                else_=0
            )).label("today_count"),
            func.sum(case(
                (ItemsArchived.updated_at >= last_30_days, 1),
                else_=0
            )).label("last_30_days_count"),
            func.sum(case(
                ((ItemsArchived.updated_at >= last_60_days) & (ItemsArchived.updated_at < last_30_days), 1),
                else_=0
            )).label("days_31_to_60_count")
        )
        .where(
            ItemsArchived.picked_by.isnot(None),
            ItemsArchived.store == store,
            ItemsArchived.is_archived == True
        )
        .group_by(ItemsArchived.picked_by)
    )

    results = await db.execute(stmt)
    result_list = results.all()

    items_picked_by_user = {
        result.picked_by: {
            "total_pick_count": result.total_pick_count,
            "today_count": result.today_count,
            "last_30_days_count": result.last_30_days_count,
            "days_31_to_60_count": result.days_31_to_60_count
        }
        for result in result_list
    }
    return items_picked_by_user

async def get_driver_delivery_stats(db: AsyncSession, store: int) -> List[DriverStats]:
        current_date = get_eastern_time()
        thirty_days_ago = current_date - timedelta(days=30)
        sixty_days_ago = current_date - timedelta(days=60)
        
        query = (
            select(
                DriverOrder.driver_name,
                func.count(DriverOrder.id).label('total_deliveries'),
                func.sum(
                    case(
                        (DriverOrder.delivered_at >= thirty_days_ago, 1),
                        else_=0
                    )
                ).label('last_30_days'),
                func.sum(
                    case(
                        (DriverOrder.delivered_at >= sixty_days_ago, 1),
                        else_=0
                    )
                ).label('last_60_days')
            )
            .filter(DriverOrder.is_delivered == True)
            .filter(DriverOrder.store == store)
            .group_by(DriverOrder.driver_name)
        )
        
        result = await db.execute(query)
        stats = result.all()
        
        return [
            DriverStats(
                driver_name=row[0],
                total_deliveries=row[1],
                last_30_days=row[2] or 0,
                last_60_days=row[3] or 0
            )
            for row in stats
        ]

async def process_return_items(db: AsyncSession, items: List[ItemReturn]):
    """Process a list of items and create return entries with location information."""
    results = []
    errors = []

    # Sort items alphabetically by item name
    sorted_items = sorted(items, key=lambda x: x.item)

    for item in sorted_items:
        try:
            # 1) Fetch all locations
            stmt = select(InvLocations).where(
                InvLocations.name == item.item,
                InvLocations.store == str(item.store),
                InvLocations.is_archived == False
            )
            result_loc = await db.execute(stmt)
            locations = result_loc.fetchall()

            # If no locations are found, use the default location placeholder
            if not locations:
                locations = [(None,)]

            location_map = [
                loc[0].full_location
                if loc and hasattr(loc[0], 'full_location') and loc[0].full_location
                else '010000000'
                for loc in locations
            ]

            # 2) Get all UPCs
            inv_stmt = select(ItemsInfo).where(ItemsInfo.item == item.item)
            inv_result = await db.execute(inv_stmt)
            inv_items = inv_result.fetchall()

            if not inv_items:
                errors.append(f"No UPC found for item {item.item}")
                continue

            # Extract all UPCs into a list
            upcs = [row[0].upc for row in inv_items if row[0].upc]
            if not upcs:
                errors.append(f"No valid UPC found for item {item.item}")
                continue

            # 3) Distribute units across locations proportionally
            total_units = item.units
            total_locations = len(location_map)

            # Even distribution and remainder calculation
            units_per_location = total_units // total_locations
            remainder = total_units % total_locations

            # Map to track location-to-units allocation
            allocation_map = {}
            for idx, full_location in enumerate(location_map):
                allocated_units = units_per_location

                # Distribute remainder units one by one to the first few locations
                if idx < remainder:
                    allocated_units += 1

                # Update the allocation map
                if allocated_units > 0:
                    allocation_map[full_location] = (
                        allocation_map.get(full_location, 0) + allocated_units
                    )

            # 4) Create entries grouped by location
            for full_location, allocated_units in allocation_map.items():
                return_entry = ItemsReturns(
                    store=item.store,
                    item=item.item,
                    units=allocated_units,
                    loc=full_location,
                    upc=json.dumps(upcs)  # Store list as JSON
                )

                db.add(return_entry)
                results.append({
                    "item": item.item,
                    "units": allocated_units,
                    "location": full_location,
                    "upc": upcs  # Return as a Python list in the response
                })

        except Exception as e:
            errors.append(f"Error processing item {item.item}: {str(e)}")

    # Commit the transaction
    if results:
        try:
            await db.commit()
        except Exception as e:
            await db.rollback()
            raise HTTPException(
                status_code=500,
                detail=f"Database error: {str(e)}"
            )

    return {
        "success": len(results) > 0,
        "errors": errors,
        "processed_items": results
    }


