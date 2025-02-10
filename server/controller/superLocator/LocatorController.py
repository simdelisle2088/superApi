from collections import Counter
from datetime import datetime, timedelta
import logging
from zoneinfo import ZoneInfo
from fastapi.responses import ORJSONResponse
from sqlalchemy import and_, delete, desc, func, or_, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.exc import SQLAlchemyError
from starlette import status
from model import QueryModel
from model.superLocator import ItemModel
from model.superLocator import InvModel
from model.superLocator.ItemModel import Item, Items, ItemsInfo, get_eastern_time
from model.superLocator.InvModel import InvLocations, Location
from model.superLocator.LoginModel import UsersLocator
from fastapi import HTTPException

def loc_to_dict(loc):
    # Convert a location string to a Location object
    # String to int to string removes the 0 at the beginning
    return Location(
        store= str(int(loc[0:2])),
        level= str(int(loc[2:3])),
        row= str(int(loc[3:5])),
        side= loc[5:6],
        column= str(int(loc[6:8])),
        shelf= loc[8:9],
        bin= str(int(loc[9:10])) if len(loc) == 10 else "",
        full_location= loc
    )


EASTERN_TIMEZONE = ZoneInfo("America/New_York")

async def get_info_by_upc(
    data: ItemModel.FindItem,
    db: AsyncSession
) -> ORJSONResponse:
    try:
        # Query the database to find the item by UPC
        result = await db.execute(
            select(ItemsInfo)
            .where(ItemsInfo.upc == data.upc)
            .limit(1)  
        )

        item = result.scalars().first()

        # Query for distinct locations
        location_result = await db.execute(
            select(InvLocations.full_location, InvLocations.updated_at)
            .where(InvLocations.upc == data.upc, InvLocations.store == data.store, InvLocations.is_archived == False)
            .distinct(InvLocations.full_location)
            .order_by(InvLocations.updated_at.desc()).limit(1)
        )

        locations_data = location_result.fetchall()
        
        if not item and not locations_data:
            # Return a 404 response if the item is not found
            return ORJSONResponse(
                status_code=status.HTTP_404_NOT_FOUND,
                content={"detail": "Localisation non trouvée pour l'item."},
            )

        locations = [locations_data[0].full_location] if locations_data else []

        # Prepare the item data to be returned
        item_data = {
            "id": getattr(item, 'id', None),
            "item": getattr(item, 'item', "inconnu"),
            "upc": data.upc,
            "description": getattr(item, 'description', "Aucune description"),
            "locations": locations
        }
        print(item_data)
        # Return the item details with locations
        return ORJSONResponse(
            status_code=status.HTTP_200_OK,
            content={'data': item_data},
        )

    except Exception as e:
        # Return a 500 response for any unexpected errors
        return ORJSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": f"Une erreur est survenue: {str(e)}"},
        )

# Verify in Order_info if a new item in the store appeared 
async def check_new_orders(store: int, level: int, db: AsyncSession) -> ORJSONResponse:
        # Calculate the time two minutes ago
        delay = get_eastern_time() - timedelta(minutes=1)

        # Use select and filter to query the data
        stmt = select(Items).filter(Items.created_at >= delay, Items.store == store)
        result = await db.execute(stmt)

        # Fetch all results
        rows = result.scalars().all()
        
        filtered_orders = [
            order for order in rows
            if (level == -1 and (item := Item.model_construct(**order.__dict__)).loc is not None)
            or (level != -1 and (item := Item.model_construct(**order.__dict__)).loc is not None and item.level == str(level))
        ]
        
        return len(filtered_orders) != 0

async def get_all_location(
        store: int,
        params: QueryModel.QueryParams, 
        db: AsyncSession
        ) -> ORJSONResponse:
    try:
        # Build query conditions
        conditions = [InvLocations.store == store, InvLocations.is_archived == False]
        if params.dateFrom:
            conditions.append(InvLocations.created_at >= params.dateFrom)
        if params.dateTo:
            conditions.append(InvLocations.created_at <= params.dateTo)
        if params.contain:
            conditions.append(
                or_(
                    InvLocations.upc.like(f'%{params.contain}%'),
                    InvLocations.name.like(f'%{params.contain}%'),
                )
            )
            
        query = select(InvLocations, UsersLocator.username).join(UsersLocator, UsersLocator.id == InvLocations.updated_by).where(*conditions)
        if params.offset: query = query.offset(params.offset)
        if params.count: query = query.limit(params.count)
        
        result = await db.execute(query.order_by(desc(InvLocations.updated_at)))
        res = result.all()

        if not res:
            # Return a 404 response if no locations are found
            return ORJSONResponse(
                status_code=status.HTTP_404_NOT_FOUND,
                content={"detail": "Localisation non trouvée pour l'item."},
            )
            
        # Transform and return the data
        data = [{
                **location.to_dict(),  # Convert InvLocations object to dict
                "updated_by_username": username  # Add the username field
            } for location, username in res
        ]

        countQuery = await db.execute(select(func.count(InvLocations.id)).where(*conditions))
        count = countQuery.scalar_one()

        # Return the localisation details
        return ORJSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "data": data,
                "length": count,
            },
        )

    except Exception as e:
        # Log the error and return a 500 response
        logging.error(f"Error fetching localisation: {e}")
        return ORJSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": "Erreur inconnue lors de la requête."},
        )

async def set_localisation(
        validated_data: InvModel.InvScan, 
        db: AsyncSession
) -> ORJSONResponse:
    try:        
        loc = loc_to_dict(validated_data.loc)
        
        if validated_data.archive:
            dict = Counter(validated_data.upc)
            for upc, count in dict.items():
                try:
                    # Calculate total items to archive (count * quantity)
                    total_to_archive = count * validated_data.quantity
                    
                    # Get IDs to be updated
                    result = await db.execute(
                        select(InvLocations.id)
                        .where(
                            and_(
                                InvLocations.upc == upc, 
                                InvLocations.full_location == validated_data.loc, 
                                InvLocations.is_archived == 0)
                        )
                        .limit(total_to_archive)
                    )
                    ids = result.scalars().all()

                    if ids:
                        stmt = (
                            update(InvLocations)
                            .where(InvLocations.id.in_(ids))
                            .values(is_archived=1, 
                                    updated_by=validated_data.updated_by)
                        )
                        await db.execute(stmt)
                        await db.commit()
                except Exception as e:
                    print(f"An error occurred: {e}")
                    await db.rollback()
                    
            return ORJSONResponse(
                status_code=status.HTTP_200_OK,
                content={"detail": "Localisation archivée avec succès."},
            )
        else:
            # Add items based on quantity for each UPC
            for i, value in enumerate(validated_data.upc):
                for _ in range(validated_data.quantity):
                    new_item = InvLocations(
                        upc=value,
                        name=validated_data.name[i],
                        store=loc.store,
                        level=loc.level,
                        row=loc.row,
                        side=loc.side,
                        column=loc.column,
                        shelf=loc.shelf,
                        bin=loc.bin,
                        full_location=loc.full_location,
                        created_by=validated_data.updated_by
                    )
                    db.add(new_item)

            try:
                await db.commit()
                return ORJSONResponse(
                    status_code=status.HTTP_201_CREATED,
                    content={"detail": "Localisation ajoutée avec succès."},
                )
            except Exception as e:
                print(f"An error occurred: {e}")
                await db.rollback()

        return ORJSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": "Une erreur est survenue lors de l'ajout de la localisation."}
        )
    except SQLAlchemyError as e:
        await db.rollback()
        return ORJSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": "Une erreur est survenue lors de l'ajout de la localisation."}
        )

async def get_locations_by_upc(upc: str, db: AsyncSession):
    location_result = await db.execute(
        select(InvLocations.full_location).filter(
            InvLocations.upc == upc, InvLocations.is_archived == False))
    locations = location_result.scalars().all()
    
    if not locations:
        raise HTTPException(
            status_code=404, detail="No locations found for the provided UPC.")
    
    return locations

async def delete_locations_by_section(
    db: AsyncSession,
    store: str,
    level: str,
    row: str,
    side: str,
) -> int:
    try:
        # Create the filter conditions
        conditions = and_(
            InvLocations.store == store,
            InvLocations.level == level,
            InvLocations.row == row,
            InvLocations.side == side,
            InvLocations.is_archived == False
        )
        
        # Get count of locations that will be archived
        count_query = select(func.count()).select_from(InvLocations).where(conditions)
        locations_count = await db.scalar(count_query)
        
        if locations_count == 0:
            raise HTTPException(
                status_code=404,
                detail="AUCUNE LOCALISATION NONE-ARCHIVÉE TROUVÉE"
            )
            
        # Create update statement
        delete_stmt = (
            delete(InvLocations)
            .where(conditions)
            .execution_options(synchronize_session=False)
        )
        
        await db.execute(delete_stmt)
        
        # Commit the transaction
        await db.commit()
        
        return locations_count
        
    except Exception as e:
        await db.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Error deleting locations: {str(e)}"
        )