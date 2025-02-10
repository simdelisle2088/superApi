import logging
from fastapi import Depends, HTTPException
from fastapi.exceptions import RequestValidationError
from fastapi.responses import ORJSONResponse
from pydantic import ValidationError
from sqlalchemy import and_, func, or_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from starlette import status
from model.superLocator import ItemModel, LoginModel
from model.superLocator.InvModel import InvLocations
from model.superLocator.ItemModel import ItemReserved, ItemUpdate, Items, Item, ItemsArchived, ItemsMissing, get_eastern_time
from utility.util import get_primary_db

# Function to retrieve all missing items from the database
async def get_all_missing_items(
    db: AsyncSession = Depends(get_primary_db),
) -> ORJSONResponse:
    query = select(Items).where(Items.is_missing == True)
    result = await db.execute(query)
    items = result.scalars().all()

    # Convert each item to the Item model and serialize to JSON
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
            is_missing=item.is_missing,
            upc=item.upc,
            picked_by=item.picked_by,
            reserved_by=item.reserved_by
        ).model_dump() for item in items
    ]

    return ORJSONResponse(content=items_data)

"""
    Retrieves picking orders for a given store ID and level.

    Args:
        validated_data (ItemModel.StoreId): The validated store ID and level.
        db (AsyncSession): The database session to use for the query.

    Returns:
        ORJSONResponse: A JSON response containing a list of picking orders.

    Example:
        >>> validated_data = ItemModel.StoreId(store='store1', level=1)
        >>> response = await get_picking_orders(validated_data, db)
        >>> print(response.content)
        [
            {'id': 1, 'store': 'store1', 'order_number': '123', ...},
            {'id': 2, 'store': 'store1', 'order_number': '456', ...},
            ...
        ]
"""
async def get_picking_orders(
    validated_data: ItemModel.StoreId, 
    db: AsyncSession,
    user_id: int,
) -> ORJSONResponse:
    try:
        store_id = validated_data.store

        query = select(Items).where(
            Items.store == store_id,
            Items.is_archived == False
        )
        result = await db.execute(query)
        picking_orders = result.scalars().all()

        if not picking_orders:
            return ORJSONResponse(
                status_code=status.HTTP_404_NOT_FOUND,
                content={"detail": "No picking orders found for the given store ID."},
            )

        # Rest of your existing logic remains the same...
        filtered_orders = [
            order for order in picking_orders
            if (validated_data.level == "-1" and (item := Item.model_construct(**order.__dict__)).loc is not None)
            or (validated_data.level != "-1" and (item := Item.model_construct(**order.__dict__)).loc is not None and item.level == validated_data.level)
        ]

        if not filtered_orders:
            return ORJSONResponse(
                status_code=status.HTTP_404_NOT_FOUND,
                content={"detail": "No picking orders found for the given store ID and level."},
            )

        response_data = []
        for order in filtered_orders:
            current_units_needed = order.units

            # Parse UPC string to handle multiple UPCs
            upcs = order.upc.strip('[]').replace("'", "").split(',')
            upcs = [upc.strip() for upc in upcs]
            upc_conditions = or_(*[InvLocations.upc == upc for upc in upcs])

            locations_query = (
                select(
                    InvLocations.full_location,
                    func.count(InvLocations.full_location).label("quantity")
                )
                .where(
                    upc_conditions,
                    InvLocations.store == str(order.store),
                    InvLocations.name == order.item,
                    InvLocations.is_archived == False,
                    InvLocations.level == (validated_data.level if validated_data.level != "-1" else InvLocations.level)
                )
                .group_by(InvLocations.full_location)
                .order_by(InvLocations.full_location)
            )

            location_result = await db.execute(locations_query)
            available_locations = location_result.fetchall()

            accumulated_locations = []
            accumulated_quantity = 0
            for loc, quantity in available_locations:
                if accumulated_quantity >= current_units_needed:
                    break
                accumulated_quantity += quantity
                accumulated_locations.append(loc)

            final_loc = accumulated_locations[0] if accumulated_locations else order.loc

            response_data.append({
                **Item.model_construct(**order.__dict__).model_dump(),
                "reserved_by": order.reserved_by,
                "loc": final_loc,
                "user_id": user_id  # Include user_id in response if needed
            })

        return ORJSONResponse(status_code=status.HTTP_200_OK, content=response_data)

    except Exception as e:
        logging.error(f"Error fetching all orders: {e}", exc_info=True)
        return ORJSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": "Unknown error occurred while retrieving orders."},
        )


"""
    Reserve an item for a user.

    Args:
    - validated_data (ItemReserved): Validated data containing item id, order number, and location.
    - user_id (int): ID of the user reserving the item.
    - db (AsyncSession): Async database session.

    Returns:
    - ORJSONResponse: Response indicating the result of the reservation.

    Raises:
    - HTTPException: If the item is not found, already reserved by another user, or if there's a validation error.

    Example:
    ```
    validated_data = ItemReserved(id=1, order_number="ORDER-123", loc="LOCATION-A")
    user_id = 1
    db = AsyncSession()
    response = await set_to_reserved(validated_data, user_id, db)
    ```
"""    
async def set_to_reserved(
    validated_data: ItemReserved,
    user_id: int,
    db: AsyncSession,
) -> ORJSONResponse:
    try:
        location_to_check = validated_data.loc.strip()
        
        # First find the item to verify its locations
        check_query = select(Items).where(
            Items.id == validated_data.id,
            Items.order_number == validated_data.order_number
        )
        result = await db.execute(check_query)
        item = result.scalars().first()

        if not item:
            return ORJSONResponse(
                status_code=status.HTTP_404_NOT_FOUND,
                content={"detail": "Pièces non trouvée."}
            )

        # Verify the location exists in item's locations
        item_locations = item.loc.split(',')
        if location_to_check not in item_locations:
            return ORJSONResponse(
                status_code=status.HTTP_400_BAD_REQUEST,
                content={"detail": f"Location {location_to_check} not found in item locations."}
            )

        if item.is_reserved and item.reserved_by != user_id:
            return ORJSONResponse(
                status_code=status.HTTP_409_CONFLICT,
                content={"detail": "Pièces déjà réservée par un autre utilisateur."}
            )

        # Update the item with only the selected location
        item.loc = location_to_check
        item.is_reserved = True
        item.reserved_by = user_id
        await db.commit()

        return ORJSONResponse(status_code=status.HTTP_200_OK, content={"detail": "Pièces est réservé."})

    except Exception as e:
        logging.error(f"Error: {str(e)}", exc_info=True)
        return ORJSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": f"Erreur: {str(e)}"}
        )

"""
    Update the status of an item.

    Args:
    - validated_data (ItemUpdate): Validated data containing item id.
    - db (AsyncSession): Async database session.
    - field (str): Field to update (e.g. is_reserved, is_available, etc.).
    - value (bool): New value for the field.

    Returns:
    - ORJSONResponse: Response indicating the result of the update.

    Raises:
    - HTTPException: If the item is not found or if there's a validation error.

    Example:
    ```
    validated_data = ItemUpdate(id=1)
    db = AsyncSession()
    field = "is_reserved"
    value = True
    response = await update_item_status(validated_data, db, field, value)
    ```
"""
async def update_item_status(
    validated_data: ItemUpdate,
    db: AsyncSession,
    field: str,
    value: bool,
) -> ORJSONResponse:
    try:
        if field == 'is_missing':
            # Handle missing status with table transfers
            if value:  # Moving to is_missing table
                result = await db.execute(
                    select(Items)
                    .where(Items.id == validated_data.id)
                    .limit(1)
                )
                
                item = result.scalars().first()
                if not item:
                    return ORJSONResponse(
                        status_code=status.HTTP_404_NOT_FOUND,
                        content={"detail": "Pièce non trouvée."},
                    )

                # Create new record for is_missing table
                missing_item = ItemsMissing(
                    store=item.store,
                    order_number=item.order_number,
                    item=item.item,
                    description=item.description,
                    units=item.units,
                    state=item.state,
                    created_at=item.created_at,
                    updated_at=get_eastern_time(),
                    updated_by=item.updated_by,
                    loc=item.loc,
                    is_reserved=item.is_reserved,
                    is_archived=item.is_archived,
                    is_missing=True,
                    upc=item.upc,
                    picked_by=item.picked_by,
                    reserved_by=item.reserved_by
                )
                
                db.add(missing_item)
                await db.delete(item)
                
            else:  # Moving back to order_info table
                result = await db.execute(
                    select(ItemsMissing)
                    .where(ItemsMissing.id == validated_data.id)
                    .limit(1)
                )
                
                missing_item = result.scalars().first()
                if not missing_item:
                    return ORJSONResponse(
                        status_code=status.HTTP_404_NOT_FOUND,
                        content={"detail": "Pièce non trouvée dans les articles manquants."},
                    )

                # Create new record for order_info table
                order_item = Items(
                    store=missing_item.store,
                    order_number=missing_item.order_number,
                    item=missing_item.item,
                    description=missing_item.description,
                    units=missing_item.units,
                    state=missing_item.state,
                    created_at=missing_item.created_at,
                    updated_at=get_eastern_time(),
                    updated_by=missing_item.updated_by,
                    loc=missing_item.loc,
                    is_reserved=missing_item.is_reserved,
                    is_archived=missing_item.is_archived,
                    is_missing=False,
                    upc=missing_item.upc,
                    picked_by=missing_item.picked_by,
                    reserved_by=missing_item.reserved_by
                )
                
                db.add(order_item)
                await db.delete(missing_item)
        
        else:
            # Handle other boolean fields (like is_reserved)
            result = await db.execute(
                select(Items)
                .where(Items.id == validated_data.id)
                .limit(1)
            )
            
            item = result.scalars().first()
            if not item:
                return ORJSONResponse(
                    status_code=status.HTTP_404_NOT_FOUND,
                    content={"detail": "Pièce non trouvée."},
                )

            # Update the specified field
            setattr(item, field, value)
            
            # If unreserving, also clear the reserved_by field
            if field == 'is_reserved' and not value:
                setattr(item, 'reserved_by', None)
            
            item.updated_at = get_eastern_time()

        await db.commit()

        # Customize messages based on the field and value
        messages = {
            'is_missing': {
                True: "Pièce marquée comme manquante.",
                False: "Pièce marquée comme en stock."
            },
            'is_reserved': {
                True: "Pièce réservée avec succès.",
                False: "Pièce libérée avec succès."
            }
        }
        
        message = messages.get(field, {}).get(value, "Statut mis à jour avec succès.")
        
        return ORJSONResponse(
            status_code=status.HTTP_200_OK,
            content={"detail": message},
        )

    except ValidationError as ve:
        logging.error(f"Validation Error: {ve}")
        await db.rollback()
        return ORJSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            content={"detail": f"Erreur de validation: {ve.errors()}"},
        )
    except Exception as e:
        logging.error(f"Error updating item status: {e}")
        await db.rollback()
        return ORJSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": "Erreur inconnue lors de la requête."},
        )

"""
    Retrieves a user by their ID.

    Args:
        validated_data (LoginModel.UserId): The user ID to retrieve.
        db (AsyncSession, optional): The database session to use. Defaults to Depends(get_primary_db).

    Returns:
        ORJSONResponse: A JSON response containing the user's ID and username, or an error message if the user is not found.

    Example:
        >>> validated_data = LoginModel.UserId(id=1)
        >>> response = await get_user_by_id(validated_data)
        >>> response.status_code
        200
        >>> response.content
        {'user_id': 1, 'username': 'john_doe'}
"""    
async def get_user_by_id(
    validated_data: LoginModel.UserId,
    db: AsyncSession = Depends(get_primary_db),
) -> ORJSONResponse:
    try:
        result = await db.execute(
            select(LoginModel.UsersLocator.id, LoginModel.UsersLocator.username)
            .where(
                and_(
                    LoginModel.UsersLocator.id == validated_data.id,
                )
            )
            .limit(1)
        )

        user = result.first()

        if not user:
            return ORJSONResponse(
                status_code=status.HTTP_404_NOT_FOUND,
                content={"detail": "Utilisateur non trouvé."},
            )

        return ORJSONResponse(
            status_code=status.HTTP_200_OK,
            content={"user_id": user.id, "username": user.username},
        )

    except Exception as e:
        logging.error(f"Error fetching user: {e}")
        return ORJSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": "Erreur inconnue lors de la requête."},
        )

"""
    Retrieves the localisation details for an item by its name and order number.

    Args:
        validated_data (ItemModel.ItemLocalisation): The item localisation data to retrieve.
        db (AsyncSession): The database session to use.

    Returns:
        ORJSONResponse: A JSON response containing the localisation details, or an error message if the item is not found.

    Example:
        >>> validated_data = ItemModel.ItemLocalisation(item='Widget', order_number=123)
        >>> response = await get_localisation(validated_data, db)
        >>> response.status_code
        200
        >>> response.content
        {'loc': 'Aisle 3, Shelf 2'}
"""
async def get_localisation(
        validated_data: ItemModel.ItemLocalisation, 
        db: AsyncSession
        ) -> ORJSONResponse:
    try:
        # Query the database to find the item by item name and order number
        result = await db.execute(
            select(Items)
            .where(
                and_(
                    Items.item == validated_data.item,
                    Items.order_number == validated_data.order_number
                )
            )
            .limit(1)  # Since we expect only one result
        )

        item = result.scalars().first()

        if not item:
            return ORJSONResponse(
                status_code=status.HTTP_404_NOT_FOUND,
                content={"detail": "Localisation non trouvée pour l'item."},
            )

        # Return the localisation details
        return ORJSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "loc": item.loc,
            },
        )

    except Exception as e:
        logging.error(f"Error fetching localisation: {e}")
        return ORJSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": "Erreur inconnue lors de la requête."},
        )
    
# Function to retrieve all localisations for a given store ID
async def get_all_localisation(
        validated_data: ItemModel.ItemAllLocalisation, 
        db: AsyncSession
        ) -> ORJSONResponse:
    try:
        # Extract store ID from the validated data
        store_id = validated_data.store

        # Perform the database query to get all orders for the store, including archived ones
        query = select(Items).where(Items.store == store_id & Items.is_missing == 0 & Items.is_archived == 0)
        result = await db.execute(query)
        all_orders = result.scalars().all()

        # Check if any orders are found
        if not all_orders:
            return ORJSONResponse(
                status_code=status.HTTP_404_NOT_FOUND,
                content={"detail": "No orders found for the given store ID."},
            )

        # Serialize the result, including the reserved_by field
        response_data = [
            {
                **Item.model_construct(**order.__dict__).model_dump(),
                "reserved_by": order.reserved_by  # Include the reserved_by field
            }
            for order in all_orders
        ]

        return ORJSONResponse(status_code=status.HTTP_200_OK, content=response_data)

    except Exception as e:
        logging.error(f"Error fetching all orders: {e}", exc_info=True)
        return ORJSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": "Unknown error occurred while retrieving orders."},
        )

async def find_item_by_upc(
    validated_data: ItemModel.FindItem,
    db: AsyncSession = Depends(get_primary_db),
) -> ORJSONResponse:
    """
    Retrieves the item details based on its UPC and returns the first item
    where units are greater than zero, ordered by created_at (oldest first).

    Args:
        validated_data (ItemModel.FindItem): Validated data containing the UPC.
        db (AsyncSession): Asynchronous database session. Defaults to the primary database.

    Returns:
        ORJSONResponse: JSON response containing the item details or an error message.
    """
    try:
        upc_search = f'{validated_data.upc}'
        
        # Query the database to find all items by UPC, ordered by created_at (oldest first)
        result = await db.execute(
            select(Items)
            .where(Items.upc.contains(upc_search),
                   Items.store == validated_data.store, Items.is_archived == False, Items.is_missing == False)
            .order_by(Items.created_at.asc())  # Order by created_at ascending (oldest first)
        )

        items = result.scalars().all()  # Get all matching items

        # Iterate through the items and find the first one with units > 0
        for item in items:
            if item.units > 0 :
                return ORJSONResponse(
                    status_code=status.HTTP_200_OK,
                    content={
                        "id": item.id,
                        "item": item.item,
                        "order_number": item.order_number,
                        "created_at": item.created_at,
                        "units": item.units,
                        "loc": item.loc,
                    },
                )

        # If no item with units > 0 was found
        logging.info(f"No item found with units > 0 for UPC: {validated_data.upc}")
        return ORJSONResponse(
            status_code=status.HTTP_404_NOT_FOUND,
            content={"detail": "Item found, but no available units."},
        )

    except Exception as e:  # Catch all other exceptions
        logging.error(f"Unexpected error fetching item by UPC: {validated_data.upc}. Error: {e}")
        return ORJSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": "Erreur inconnue lors de la requête."},
        )
    
async def pick_item_by_upc(
    validated_data: ItemModel.ItemPicked,
    db: AsyncSession = Depends(get_primary_db),
) -> ORJSONResponse:
    try:
        upc_search = f'{validated_data.upc}'

        # Fetch the item based on the combination of id, upc, and order_number
        result = await db.execute(
            select(Items).where(
                Items.id == validated_data.id,
                Items.order_number == validated_data.order_number,
                Items.upc.contains(upc_search)
            )
        )
        item = result.scalars().first()

        if not item:
            raise HTTPException(status_code=404, detail="Item not found")

        units_to_pick = validated_data.units_to_pick
        remaining_units = units_to_pick

        # Fetch all locations for the given UPC, grouped by full_location
        result_locs = await db.execute(
            select(InvLocations.full_location)
            .where(
                InvLocations.upc == upc_search,
                InvLocations.store == item.store,
                InvLocations.is_archived == False
            )
            .group_by(InvLocations.full_location)
            .order_by(InvLocations.full_location)
        )
        locations = result_locs.scalars().all()

        if not locations:
            if item.units >= units_to_pick:
                # Update units and check if we need to archive the item
                item.units -= units_to_pick
                if item.units <= 0:
                    # Create a new record in ItemsArchived table
                    archived_item = ItemsArchived(
                        store=item.store,
                        order_number=item.order_number,
                        item=item.item,
                        description=item.description,
                        units=units_to_pick,
                        state='picked',
                        updated_by=validated_data.picked_by,
                        loc=item.loc,
                        is_reserved=item.is_reserved,
                        is_archived=True,
                        is_missing=False,
                        upc=item.upc,
                        picked_by=validated_data.picked_by,
                        reserved_by=item.reserved_by
                    )
                    db.add(archived_item)
                    # Delete the original item
                    await db.delete(item)
                else:
                    item.updated_at = get_eastern_time()
                    item.picked_by = validated_data.picked_by

                await db.commit()

                return ORJSONResponse(
                    status_code=200,
                    content={"message": f"{units_to_pick} units picked successfully without associated locations."},
                )
            else:
                raise HTTPException(
                    status_code=400,
                    detail=f"Not enough stock to pick {units_to_pick} units. Only {item.units} units available."
                )
        
        # Process each location and archive rows until required units are picked
        for full_location in locations:
            result_rows = await db.execute(
                select(InvLocations)
                .where(
                    InvLocations.full_location == full_location,
                    InvLocations.upc == upc_search,
                    InvLocations.store == item.store,
                    InvLocations.is_archived == False
                )
                .order_by(InvLocations.created_at)
            )
            location_rows = result_rows.scalars().all()

            if not location_rows:
                continue

            for location_row in location_rows:
                location_row.is_archived = True
                location_row.updated_at = get_eastern_time()
                location_row.updated_by = validated_data.picked_by
                remaining_units -= 1

                if remaining_units <= 0:
                    break

            if remaining_units <= 0:
                break

        if remaining_units > 0:
            raise HTTPException(
                status_code=400,
                detail=f"Not enough stock to pick {units_to_pick} units. {units_to_pick - remaining_units} units picked."
            )

        # Update the item and check if it needs to be archived
        item.units -= units_to_pick
        if item.units <= 0:
            # Create a new record in ItemsArchived table
            archived_item = ItemsArchived(
                store=item.store,
                order_number=item.order_number,
                item=item.item,
                description=item.description,
                units=units_to_pick,
                state='picked',
                updated_by=validated_data.picked_by,
                loc=item.loc,
                is_reserved=item.is_reserved,
                is_archived=True,
                is_missing=False,
                upc=item.upc,
                picked_by=validated_data.picked_by,
                reserved_by=item.reserved_by
            )
            db.add(archived_item)
            # Delete the original item
            await db.delete(item)
        else:
            item.updated_at = get_eastern_time()
            item.picked_by = validated_data.picked_by

        await db.commit()

        return ORJSONResponse(
            status_code=200,
            content={"message": f"{units_to_pick} units picked successfully"}
        )

    except HTTPException:
        # Re-raise HTTP exceptions to preserve their status codes
        raise
    except Exception as e:
        logging.error(f"Error picking item: {e}")
        await db.rollback()
        raise HTTPException(
            status_code=500,
            detail="Une erreur est survenue lors de la sélection de l'article"
        )
    
async def by_pass_item_scan(
    validated_data: ItemModel.ByPassItem,
    db: AsyncSession = Depends(get_primary_db),
) -> ORJSONResponse:
    try:
        # Fetch the item based on the combination of item and order_number
        result = await db.execute(
            select(Items).where(
                Items.item == validated_data.item,
                Items.order_number == validated_data.order_number,
            )
        )
        item = result.scalars().first()

        if not item:
            raise HTTPException(status_code=404, detail="Item not found")
        
        # Ensure there are units to process
        if item.units <= 0:
            raise HTTPException(status_code=400, detail="Item has no units left to process")
        
        units_to_archive = item.units
        remaining_units = units_to_archive
        archived_locations = []

        # Fetch all non-archived locations for the item, grouped by full_location
        result_locs = await db.execute(
            select(
                InvLocations.full_location,
                func.count(InvLocations.id).label("available_units"),
            )
            .where(
                InvLocations.store == item.store,
                InvLocations.name == item.item,
                InvLocations.is_archived == False,
            )
            .group_by(InvLocations.full_location)
            .order_by(InvLocations.full_location)
        )
        locations = result_locs.fetchall()

        if not locations:
            # If no locations are available, archive and delete the item
            # Create archive record before deletion
            archived_item = ItemsArchived(
                store=item.store,
                order_number=item.order_number,
                item=item.item,
                description=item.description,
                units=item.units,
                state='bypassed',
                updated_by=validated_data.picked_by,
                loc=item.loc,
                is_reserved=item.is_reserved,
                is_archived=True,
                is_missing=False,
                upc=item.upc,
                picked_by=validated_data.picked_by,
                reserved_by=item.reserved_by
            )
            db.add(archived_item)
            
            # Store units for response
            item_units = item.units
            
            # Delete the original item
            await db.delete(item)
            await db.commit()

            return ORJSONResponse(
                status_code=200,
                content={
                    "message": f"{item_units} units processed successfully without associated locations.",
                },
            )
        
        # Process each location until all units are handled
        for location in locations:
            full_location, available_units = location

            # Determine how many units to process from this location
            units_from_location = min(available_units, remaining_units)
            remaining_units -= units_from_location

            # Fetch and update location rows
            result_rows = await db.execute(
                select(InvLocations)
                .where(
                    InvLocations.full_location == full_location,
                    InvLocations.store == item.store,
                    InvLocations.name == item.item,
                    InvLocations.is_archived == False,
                )
                .order_by(InvLocations.created_at)
                .limit(units_from_location)
            )
            location_rows = result_rows.scalars().all()

            for row in location_rows:
                row.is_archived = True
                row.updated_at = get_eastern_time()
                row.updated_by = validated_data.picked_by

            archived_locations.append(full_location)

            if remaining_units <= 0:
                break

        # Check if we have enough units
        if remaining_units > 0:
            raise HTTPException(
                status_code=400,
                detail=f"Not enough stock to process {units_to_archive} units. Only {units_to_archive - remaining_units} units processed.",
            )

        # Create archive record before deletion
        archived_item = ItemsArchived(
            store=item.store,
            order_number=item.order_number,
            item=item.item,
            description=item.description,
            units=units_to_archive,
            state='bypassed',
            updated_by=validated_data.picked_by,
            loc=item.loc,
            is_reserved=item.is_reserved,
            is_archived=True,
            is_missing=False,
            upc=item.upc,
            picked_by=validated_data.picked_by,
            reserved_by=item.reserved_by
        )
        db.add(archived_item)
        
        # Store locations for response
        processed_locations = archived_locations.copy()
        
        # Delete the original item
        await db.delete(item)
        await db.commit()

        return ORJSONResponse(
            status_code=200,
            content={
                "message": f"{units_to_archive} units processed successfully.",
                "archived_locations": processed_locations,
            },
        )

    except HTTPException:
        # Re-raise HTTP exceptions to preserve their status codes
        raise
    except Exception as e:
        logging.error(f"Error processing item: {e}")
        await db.rollback()
        raise HTTPException(
            status_code=500,
            detail="Une erreur est survenue lors du traitement de l'article"
        )


async def get_upc(
        validated_data: ItemModel.ItemLocalisation, 
        db: AsyncSession
        ) -> ORJSONResponse:
    """
    Retrieves the UPC code for an item based on its name and order number.

    Args:
        validated_data (ItemModel.ItemLocalisation): Validated data containing the item's name and order number.
        db (AsyncSession): The database session to use for the query.

    Returns:
        ORJSONResponse: A JSON response containing the UPC code of the item or an error message if the item is not found.

    Example:
        >>> validated_data = ItemModel.ItemLocalisation(item='Widget', order_number='123')
        >>> response = await get_upc(validated_data, db)
        >>> response.status_code
        200
        >>> response.content
        {'upc': '012345678912'}
    """
    try:
        # Query the database to find the item by item name and order number
        result = await db.execute(
            select(Items)
            .where(
                and_(
                    Items.item == validated_data.item,
                    Items.order_number == validated_data.order_number
                )
            )
            .limit(1)  # Since we expect only one result
        )

        item = result.scalars().first()

        if not item:
            return ORJSONResponse(
                status_code=status.HTTP_404_NOT_FOUND,
                content={"detail": "Localisation non trouvée pour l'item."},
            )

        # Return the UPC code of the item
        return ORJSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "upc": item.upc,
            },
        )

    except Exception as e:
        logging.error(f"Error fetching localisation: {e}")
        return ORJSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": "Erreur inconnue lors de la requête."},
        )


