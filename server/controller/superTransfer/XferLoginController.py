from typing import Dict, List
from fastapi.responses import ORJSONResponse
from controller.superDeliver.DriverController import argon2_strong_hash, create_jwe, verify_password
from sqlalchemy.future import select
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import HTTPException, status
from sqlalchemy import and_, update
import logging

from model.superTransfer.XferItemModel import TransferItemSchema, TransferItems
from model.superTransfer.XferLoginModel import UsersXfer, XferCreate, XferLogin

async def create_xfer(
    validated_data: XferCreate,
    db: AsyncSession,
) -> ORJSONResponse:
    """
    Asynchronously creates a new locator user in the database.

    Args:
        validated_data (LocatorCreate): The validated data for creating a new locator.
        db (AsyncSession): The asynchronous database session.

    Returns:
        ORJSONResponse: The response object containing the status and message.
    """
    try:
        # Asynchronously check if the username already exists
        result = await db.execute(
            select(UsersXfer).where(UsersXfer.username == validated_data.username).limit(1)
        )
        if result.scalars().first():
            return ORJSONResponse(
                status_code=status.HTTP_400_BAD_REQUEST,
                content={"detail": "Nom d'utilisateur déjà utilisé."},
            )

        # Hash the provided password
        hashed_password = argon2_strong_hash(validated_data.password)

        # Create a new UsersLocator instance
        new_locator = UsersXfer(
            username=validated_data.username,
            password=hashed_password,
            store=validated_data.store,
        )

        # Add the new locator to the database and commit the transaction
        db.add(new_locator)
        await db.commit()

        return ORJSONResponse(
            content={"detail": "Localisateur créé!"},
            status_code=status.HTTP_201_CREATED,
        )
    except Exception as e:
        # Rollback the transaction in case of an error
        await db.rollback()
        logging.warning(f"Database error in create Localisateur: {e}")
        return ORJSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": "Erreur inconnue dans la base de données."},
        )
        
async def login_xfer(
    validated_data: XferLogin,
    db: AsyncSession,
) -> ORJSONResponse:
    """
    Asynchronously logs in a locator user by verifying credentials and generating a JWE token.

    Args:
        validated_data (LocatorLogin): The validated data for logging in.
        db (AsyncSession): The asynchronous database session.

    Returns:
        ORJSONResponse: The response object containing the status, token, and user details.
    """
    try:
        # Asynchronously query the database for the user
        result = await db.execute(
            (
                select(UsersXfer)
                .where(
                    and_(
                        UsersXfer.username == validated_data.username
                    )
                )
                .limit(1)
            )
        )
        user = result.scalars().first()

        if not user:
            logging.warning(f"No user found: {validated_data.username}")
            return ORJSONResponse(
                status_code=status.HTTP_401_UNAUTHORIZED,
                content={"detail": "Nom d'utilisateur ou mot de passe incorrect."},
            )

        # Verify the provided password against the stored hashed password
        if not verify_password(user.password, validated_data.password):
            logging.warning(f"Invalid password for: {validated_data.username}")
            return ORJSONResponse(
                status_code=status.HTTP_401_UNAUTHORIZED,
                content={"detail": "Nom d'utilisateur ou mot de passe incorrect."},
            )
        
        # Commit the transaction and refresh the user instance
        await db.commit()
        await db.refresh(user)

        # Create a JWE token for the user
        token = create_jwe(user.id)

        return ORJSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "token": token,
                "storeId": user.store,
                "username": user.username,
                "user_id": user.id
            },
        )
    except Exception as e:
        # Rollback the transaction in case of an error
        await db.rollback()
        logging.warning(f"Database error for: {validated_data.username} - {e}")
        return ORJSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": "Nom d'utilisateur ou mot de passe incorrect."},
        )
    
async def get_transfer_items_by_store(db: AsyncSession, store: int) -> List[TransferItems]:
    result = await db.execute(
        select(TransferItems).where(
            TransferItems.store == store, 
            TransferItems.is_archived == False
        )
    )
    return result.scalars().all()

async def get_categorized_transfers(db: AsyncSession) -> Dict[str, List[TransferItems]]:
    # Fetch all non-archived transfer items
    result = await db.execute(
        select(TransferItems).where(TransferItems.is_archived == False)
    )
    transfers = result.scalars().all()
    
    # Group transfers into categories
    categorized_transfers = {}
    for transfer in transfers:
        category = f"0000{transfer.store}{transfer.tran_to_store}"
        if category not in categorized_transfers:
            categorized_transfers[category] = []
        categorized_transfers[category].append(transfer)
    
    return categorized_transfers

async def get_transfers_by_customer(db: AsyncSession, customer: str) -> List[TransferItemSchema]:
    try:
        print(f"Fetching transfers for customer {customer}")
        # Execute the query to fetch transfers filtered by store and customer
        result = await db.execute(
            select(TransferItems).where(
                TransferItems.customer == customer,
                TransferItems.is_archived == False
            )
        )
        
        # Extract the list of transfer items
        transfers = result.scalars().all()
        
        # Return the list of serialized transfer items
        return [TransferItemSchema.from_orm(transfer) for transfer in transfers]
    
    except Exception as e:
        # Log the error and re-raise it for debugging purposes
        print(f"Error fetching transfers  customer {customer}: {e}")
        raise

async def archive_transfer_item(db: AsyncSession, customer: str, upc: str) -> bool:
    # Fetch the transfer item
    result = await db.execute(
        select(TransferItems).where(
            TransferItems.customer == customer,
            TransferItems.upc == upc,
            TransferItems.is_archived == False
        )
    )

    transfer_item = result.scalars().first()

    if not transfer_item:
        print(f"No unarchived transfer item found for customer {customer} and UPC {upc}")
        raise HTTPException(status_code=404, detail="Transfer item not found or already archived.")

    # Decrement the quantity
    if transfer_item.qty_selling_units > 0:
        transfer_item.qty_selling_units -= 1

    # Only archive if quantity reaches 0
    if transfer_item.qty_selling_units == 0:
        transfer_item.is_archived = True

        # Archive in related inventory locations
        result_loc = await db.execute(
            select(TransferItems).where(
                TransferItems.upc == upc,
                TransferItems.store == transfer_item.store,
                TransferItems.is_archived == False
            )
        )

        location_item = result_loc.scalars().first()

        if location_item:
            location_item.is_archived = True

    # Commit the changes
    await db.commit()

    print(f"Item with UPC {upc} archived for customer {customer}")
    return True

async def bypass_batch_scan(db: AsyncSession, customer: str, upc: str) -> bool:
        """
        Bypasses batch scan for a specific item by setting qty_selling_units to 0 and archiving it.
        """
        # Fetch the transfer item
        result = await db.execute(
            select(TransferItems).where(
                TransferItems.customer == customer,
                TransferItems.upc == upc,
                TransferItems.is_archived == False
            )
        )

        transfer_item = result.scalars().first()

        if not transfer_item:
            print(f"No unarchived transfer item found for customer {customer} and UPC {upc}")
            return False

        # Set qty_selling_units to 0 and archive the item
        transfer_item.qty_selling_units = 0
        transfer_item.is_archived = True

        # Commit changes
        await db.commit()

        print(f"Item with UPC {upc} bypassed and archived for customer {customer}")
        return True