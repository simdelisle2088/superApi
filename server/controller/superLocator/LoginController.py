import datetime
from typing import Dict, Optional
from fastapi.responses import ORJSONResponse
from controller.superDeliver.DriverController import argon2_strong_hash, create_jwe, verify_password
from sqlalchemy.future import select
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import HTTPException, status
from sqlalchemy import and_
import logging
from model.UserDeviceModel import UserDevice
from model.superLocator.ItemModel import get_eastern_time
from model.superLocator.LoginModel import (
    LocatorCreate, 
    LocatorLogin, 
    UsersLocator
)

async def create_locator(
    validated_data: LocatorCreate,
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
            select(UsersLocator).where(UsersLocator.username == validated_data.username).limit(1)
        )
        if result.scalars().first():
            return ORJSONResponse(
                status_code=status.HTTP_400_BAD_REQUEST,
                content={"detail": "Nom d'utilisateur déjà utilisé."},
            )

        # Hash the provided password
        hashed_password = argon2_strong_hash(validated_data.password)

        # Create a new UsersLocator instance
        new_locator = UsersLocator(
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

async def verify_device(
    db: AsyncSession,
    user_id: int,
    device_info: Dict
) -> Optional[UserDevice]:
    """
    Verify if a device is already registered to a user.
    
    This function checks if the device is in our database and is still active.
    We use this to track which devices are accessing the system and detect potential
    security issues like unauthorized device usage.
    """
    try:
        # Query for an active device matching both user and device ID
        result = await db.execute(
            select(UserDevice).where(
                and_(
                    UserDevice.user_id == user_id,
                    UserDevice.device_id == device_info['deviceId'],
                    UserDevice.is_active == True
                )
            )
        )
        return result.scalars().first()
    except Exception as e:
        logging.error(f"Error verifying device for user {user_id}: {e}")
        return None

async def register_device(
    db: AsyncSession,
    user_id: int,
    device_info: Dict
) -> UserDevice:
    """
    Register a new device for a user.
    
    This creates a new device record in our database, allowing us to track
    which devices are authorized to access the system. This helps with
    security auditing and detecting suspicious login patterns.
    """
    try:
        # Create a new device record with all available information
        new_device = UserDevice(
            user_id=user_id,
            device_id=device_info['deviceId'],
            device_type=device_info['deviceType'],
            device_model=device_info.get('model'),
            manufacturer=device_info.get('manufacturer'),
            platform=device_info['platform'],
            platform_version=device_info.get('platformVersion'),
            fingerprint=device_info.get('fingerprint'),
            last_login=get_eastern_time(),
            is_active=True
        )
        db.add(new_device)
        await db.flush()
        return new_device
    except Exception as e:
        logging.error(f"Error registering device for user {user_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error registering device"
        )

async def update_device_login(
    db: AsyncSession,
    device: UserDevice
) -> None:
    """
    Update the last login time for a device.
    
    This helps us track device usage patterns and maintain an audit trail
    of when devices are accessing the system.
    """
    try:
        device.last_login = get_eastern_time()
        await db.flush()
    except Exception as e:
        logging.error(f"Error updating device login time: {e}")
        # We don't raise an exception here as this is not critical to the login process

async def login_locator(
    validated_data: LocatorLogin,
    db: AsyncSession,
) -> ORJSONResponse:
    """
    Handle the login process for a locator user, including device verification.
    
    This function performs several important steps:
    1. Validates the user credentials
    2. Verifies or registers the device being used
    3. Creates a secure token for the session
    4. Maintains an audit trail of logins
    """
    try:
        # Find the user in the database
        result = await db.execute(
            select(UsersLocator)
            .where(UsersLocator.username == validated_data.username)
            .limit(1)
        )
        user = result.scalars().first()

        if not user:
            logging.warning(f"Login attempt with non-existent username: {validated_data.username}")
            return ORJSONResponse(
                status_code=status.HTTP_401_UNAUTHORIZED,
                content={"detail": "Nom d'utilisateur ou mot de passe incorrect."},
            )

        # Verify password
        if not verify_password(user.password, validated_data.password):
            logging.warning(f"Invalid password attempt for user: {validated_data.username}")
            return ORJSONResponse(
                status_code=status.HTTP_401_UNAUTHORIZED,
                content={"detail": "Nom d'utilisateur ou mot de passe incorrect."},
            )

        # Handle device verification and registration
        device_info = validated_data.deviceInfo.dict()
        existing_device = await verify_device(db, user.id, device_info)
        
        if existing_device:
            # Update last login time for existing device
            await update_device_login(db, existing_device)
            device_status = "existing"
        else:
            # Register new device
            existing_device = await register_device(db, user.id, device_info)
            device_status = "new"

        # Create enhanced token with user and device information
        token_payload = {
            "user_id": user.id,
            "device_id": existing_device.device_id,
            "device_type": existing_device.device_type
        }
        token = create_jwe(token_payload)

        # Commit all changes
        await db.commit()

        return ORJSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "token": token,
                "storeId": user.store,
                "username": user.username,
                "user_id": user.id,
                "deviceStatus": device_status
            },
        )

    except Exception as e:
        await db.rollback()
        logging.error(f"Login error for user {validated_data.username}: {str(e)}")
        return ORJSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": "Une erreur s'est produite lors de la connexion."},
        )
    
