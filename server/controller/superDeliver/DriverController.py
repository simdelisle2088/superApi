from asyncio import exceptions
from typing import Dict
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from fastapi.responses import ORJSONResponse
from datetime import timedelta, datetime
from argon2 import PasswordHasher, Type
from pyrate_limiter import Any
import pytz
from sqlalchemy.future import select
from fastapi import HTTPException, status, Request, Depends
from sqlalchemy import and_, case, func, text
import logging
import base64
import orjson
import os
from sqlalchemy.orm import aliased
from model.superDeliver.DriverModel import (
    Driver, 
    DriverCreateOrUpdate, 
    DriverLogin, 
    DriverId, 
    StoreId
)
from model.superDeliver.DriverOrderModel import DriverOrder
from utility.util import (
    AsyncSession,
    PRIVATE_KEY,
    JWE_SECRET_KEY,
    PUBLIC_KEY,
    ARGON2_SECRET_KEY,
    HERE_SDK_KEY_ID,
    HERE_SDK_KEY,
    get_primary_db,
)

# Authentication / Security Utilities

def argon2_strong_hash(password: str) -> str:
    """
    Hashes a password using Argon2 with a secret key (pepper).
    
    Args:
        password (str): The password to hash.
    
    Returns:
        str: The hashed password.
    """
    # Create the peppered password
    peppered_password = f"{password}{ARGON2_SECRET_KEY}"

    # Create a PasswordHasher with custom parameters
    hasher = PasswordHasher(
        time_cost=1,
        memory_cost=65536,
        parallelism=1,
        hash_len=32,
        salt_len=16,
        type=Type.ID,
    )

    # Hash the password
    hash = hasher.hash(peppered_password)
    return hash

EASTERN_TZ = pytz.timezone('US/Eastern')
    
def create_jwe(driver_id: int) -> str:
    """
    Creates a JSON Web Encryption (JWE) token for a driver.
    
    Args:
        driver_id (int): The ID of the driver.
    
    Returns:
        str: The JWE token.
    """
    # Calculate the expiration date as a Unix timestamp
    expiration_date = datetime.now(EASTERN_TZ) + timedelta(hours=144)
    expiration_timestamp = int(expiration_date.timestamp())

    # Create the claims
    claims = {"sub": driver_id, "exp": expiration_timestamp}

    # Create a JWT payload (claims) in Base85 encoding
    payload_b85 = base64.b85encode(orjson.dumps(claims)).decode()
    jwt = f"{payload_b85}"

    # Sign the JWT with Ed25519
    signature = PRIVATE_KEY.sign(jwt.encode())
    signature_b85 = base64.b85encode(signature).decode()
    signed_jwt = f"{jwt}.{signature_b85}"

    # Generate a random nonce for AES-GCM encryption
    nonce = os.urandom(12)

    # Encrypt the signed JWT with AES-GCM using the secret key and nonce
    aesgcm = AESGCM(JWE_SECRET_KEY)
    ciphertext = aesgcm.encrypt(nonce, signed_jwt.encode(), None)

    # Encode the nonce and ciphertext to Base85 for URL-safe transmission
    iv_b85 = base64.b85encode(nonce).decode()
    ciphertext_b85 = base64.b85encode(ciphertext).decode()

    # Create the final JWE string by concatenating the encoded nonce and ciphertext
    jwe = f"{iv_b85}.{ciphertext_b85}"
    
    return jwe

def authenticate(request: Request, jwe: str) -> bool:
    """
    Authenticates a request using a JWE token.
    
    Args:
        request (Request): The incoming request.
        jwe (str): The JWE token.
    
    Returns:
        bool: True if authentication is successful, False otherwise.
    """
    try:
        # Split the JWE into IV and ciphertext
        iv_b85, ciphertext_b85 = jwe.split(".")
        iv = base64.b85decode(iv_b85)
        ciphertext = base64.b85decode(ciphertext_b85)

        # Decrypt the JWE
        aesgcm = AESGCM(JWE_SECRET_KEY)
        signed_jwt = aesgcm.decrypt(iv, ciphertext, None).decode()

        # Split the signed JWT into payload and signature
        payload_b85, signature_b85 = signed_jwt.rsplit(".", 1)
        signature = base64.b85decode(signature_b85)

        # Verify the signature using Ed25519
        PUBLIC_KEY.verify(signature, payload_b85.encode())

        # Decode the payload
        payload = orjson.loads(base64.b85decode(payload_b85))

        # Check if the current time is before the expiration time
        current_time = datetime.now(EASTERN_TZ).timestamp()
        if current_time >= payload["exp"]:
            return False

        # Set the driver_id in the request state
        request.state.driver_id = int(payload["sub"])
        return True
    except Exception as e:
        print(f"Authentication failed: {e}")
        return False

def verify_password(stored_password: str, provided_password: str) -> bool:
    """
    Verifies a provided password against a stored hashed password.
    
    Args:
        stored_password (str): The stored hashed password.
        provided_password (str): The provided password.
    
    Returns:
        bool: True if the password is correct, False otherwise.
    """
    peppered_password = f"{provided_password}{ARGON2_SECRET_KEY}"
    try:
        hasher = PasswordHasher()
        hasher.verify(stored_password, peppered_password)
        return True
    except exceptions.VerifyMismatchError:
        return False

async def login(
    validated_data: DriverLogin,
    db: AsyncSession,
) -> ORJSONResponse:
    """
    Handles the login process for a driver.
    
    Args:
        validated_data (DriverLogin): The validated login data.
        db (AsyncSession): The database session.
    
    Returns:
        ORJSONResponse: The response containing the JWE token and other details.
    """
    try:
        # Asynchronously query the database
        result = await db.execute(
            select(Driver).where(
                and_(
                    Driver.username == validated_data.username,
                    Driver.is_active == 1,
                )
            ).limit(1)
        )
        user = result.scalars().first()

        if not user:
            logging.warning(f"No user found: {validated_data.username}")
            return ORJSONResponse(
                status_code=status.HTTP_401_UNAUTHORIZED,
                content={"detail": "Nom d'utilisateur ou mot de passe incorrect."},
            )

        # Verify the password
        if not verify_password(user.password, validated_data.password):
            logging.warning(f"Invalid password for: {validated_data.username}")
            return ORJSONResponse(
                status_code=status.HTTP_401_UNAUTHORIZED,
                content={"detail": "Nom d'utilisateur ou mot de passe incorrect."},
            )

        await db.commit()
        await db.refresh(user)

        token = create_jwe(user.id)

        return ORJSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "token": token,
                "sdk_key_id": HERE_SDK_KEY_ID,
                "sdk_key": HERE_SDK_KEY,
                "storeId": user.store, 
                "driverId": user.id, 
            },
        )
    except Exception as e:
        await db.rollback()
        logging.warning(f"Database error for: {validated_data.username} - {e}")
        return ORJSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": "Nom d'utilisateur ou mot de passe incorrect."},
        )

async def get_driver(driver_id: int, db: AsyncSession = Depends(get_primary_db)):
    """
    Retrieves a driver by their ID.
    
    Args:
        driver_id (int): The ID of the driver.
        db (AsyncSession): The database session.
    
    Returns:
        ORJSONResponse: The response containing the driver details.
    """
    try:
        result = await db.execute(select(Driver).filter(Driver.id == driver_id))
        driver = result.scalars().first()

        if not driver:
            return ORJSONResponse(
                status_code=status.HTTP_404_NOT_FOUND,
                content={"detail": "No drivers found."},
            )

        return ORJSONResponse(
                content={"drivers": driver},
                status_code=status.HTTP_200_OK,
            )
    except Exception as e:
        await db.rollback()
        logging.error(f"Database error in retrieving drivers: {e}")
        return ORJSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": "Unknown database error."},
        )
class OrderAggregator:
    def __init__(self):
        self.cache = {}
    
    def add_order(self, driver: str, date: datetime.date, route: str, 
                 order_number: str, client_name: str, delivery_time: int):
        if driver not in self.cache:
            self.cache[driver] = {}
            
        if date not in self.cache[driver]:
            self.cache[driver][date] = {
                "date": date,
                "order_count": 0,
                "routes": {}
            }
            
        if route not in self.cache[driver][date]["routes"]:
            self.cache[driver][date]["routes"][route] = {
                "route": route,
                "order_numbers": [],
                "client_names": [],
                "delivery_times": []
            }
            
        route_data = self.cache[driver][date]["routes"][route]
        route_data["order_numbers"].append(order_number)
        route_data["client_names"].append(client_name)
        if delivery_time is not None:
            route_data["delivery_times"].append(self._format_delivery_time(delivery_time))
        self.cache[driver][date]["order_count"] += 1

    @staticmethod
    def _format_delivery_time(seconds: int) -> str:
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        return f"{hours}h {minutes}m"

    def get_formatted_data(self) -> Dict:
        formatted_data = {}
        for driver, dates in self.cache.items():
            formatted_data[driver] = []
            for date, date_data in sorted(dates.items()):
                formatted_entry = {
                    "date": date,
                    "order_count": date_data["order_count"],
                    "routes": []
                }
                for route_data in date_data["routes"].values():
                    formatted_entry["routes"].append({
                        "route": route_data["route"],
                        "order_numbers": route_data["order_numbers"],
                        "client_names": route_data["client_names"],
                        "delivery_times": route_data["delivery_times"]
                    })
                formatted_data[driver].append(formatted_entry)
        return formatted_data

async def get_order_count_per_day(
        validated_data: StoreId,
        db: AsyncSession) -> ORJSONResponse:
    try:
        # Create a CTE for active drivers to reduce joins
        drivers_cte = (
            select(Driver.username)
            .where(Driver.store == validated_data.store)
            .cte()
        )

        # Optimize the main query
        query = (
            select(
                DriverOrder.driver_name,
                func.date(DriverOrder.created_at).label('order_date'),
                DriverOrder.route,
                DriverOrder.order_number,
                DriverOrder.client_name,
                case(
                    (DriverOrder.delivered_at.isnot(None),
                     func.timestampdiff(text('SECOND'), DriverOrder.created_at, DriverOrder.delivered_at)),
                    else_=None
                ).label('delivery_time')
            )
            .join(drivers_cte, drivers_cte.c.username == DriverOrder.driver_name)
            .where(
                and_(
                    DriverOrder.created_at >= func.date_sub(func.current_date(), text('INTERVAL 30 DAY')),
                    DriverOrder.driver_name.isnot(None)
                )
            )
            .order_by(
                DriverOrder.driver_name,
                DriverOrder.created_at,
                DriverOrder.route
            )
            .with_hint(DriverOrder, 'FORCESEEK(IX_driver_orders_driver_store)', 'mssql')
        )

        result = await db.execute(query)
        orders = result.fetchall()

        if not orders:
            return ORJSONResponse(
                status_code=status.HTTP_404_NOT_FOUND,
                content={"detail": "No orders found for the specified store."},
            )

        # Use the optimized aggregator
        aggregator = OrderAggregator()
        
        # Process results in batches
        for order in orders:
            aggregator.add_order(
                driver=order.driver_name,
                date=order.order_date,
                route=order.route,
                order_number=order.order_number,
                client_name=order.client_name,
                delivery_time=order.delivery_time
            )

        return ORJSONResponse(
            content={"orders_per_day": aggregator.get_formatted_data()},
            status_code=status.HTTP_200_OK,
        )

    except Exception as e:
        logging.error(f"Database error in retrieving orders per day: {e}")
        return ORJSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": "Unknown database error."},
        )
    
async def get_all_drivers(
        validated_data: StoreId,
        db: AsyncSession) -> ORJSONResponse:
    """
    Retrieves all drivers for a specific store along with the count of their orders.
    
    Args:
        validated_data (StoreId): The validated store data.
        db (AsyncSession): The database session.
    
    Returns:
        ORJSONResponse: The response containing the list of drivers and their order counts.
    """
    try:
        # Aliasing DriverOrder for join
        DriverOrderAlias = aliased(DriverOrder)
        
        # Query to get drivers and count of orders per driver
        result = await db.execute(
            select(
                Driver.id,
                Driver.is_active,
                Driver.username,
                Driver.store,
                Driver.created_at,
                func.count(DriverOrderAlias.id).label('order_count')
            )
            .outerjoin(DriverOrderAlias, Driver.username == DriverOrderAlias.driver_name)
            .where(Driver.store == validated_data.store)
            .group_by(Driver.id)
        )
        
        drivers = result.all()

        if not drivers:
            return ORJSONResponse(
                status_code=status.HTTP_404_NOT_FOUND,
                content={"detail": "No drivers found."},
            )

        drivers_data = [
            {
                "id": driver.id,
                "is_active": driver.is_active,
                "username": driver.username,
                "store": driver.store,
                "created_at": driver.created_at,
                "order_count": driver.order_count,
            }
            for driver in drivers
        ]

        return ORJSONResponse(
            content={"drivers": drivers_data},
            status_code=status.HTTP_200_OK,
        )

    except Exception as e:
        await db.rollback()
        logging.error(f"Database error in retrieving drivers: {e}")
        return ORJSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": "Unknown database error."},
        )

async def get_driver(
       validated_data: DriverId,
        db: AsyncSession) -> ORJSONResponse: 
    """
    Retrieves a driver by their ID.
    
    Args:
        validated_data (DriverId): The validated driver ID data.
        db (AsyncSession): The database session.
    
    Returns:
        ORJSONResponse: The response containing the driver details.
    """
    try:
        result = await db.execute(select(Driver)
                    .where(
                        Driver.id == validated_data.driver_id,
                    ).limit(1)
                )
        driver = result.scalars().first()

        if not driver:
            return ORJSONResponse(
                status_code=status.HTTP_404_NOT_FOUND,
                content={"detail": "No drivers found."},
            )

        driver_data = {
                    "id": driver.id,
                    "is_active": driver.is_active,
                    "username": driver.username,
                    "store": driver.store,
                    "created_at": driver.created_at,
                }

        return ORJSONResponse(
            content={"drivers": driver_data},
            status_code=status.HTTP_200_OK,
        )
    except Exception as e:
        await db.rollback()
        logging.error(f"Database error in retrieving drivers: {e}")
        return ORJSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": "Unknown database error."},
        )

async def create_driver(
    validated_data: DriverCreateOrUpdate,
    db: AsyncSession,
) -> ORJSONResponse:
    """
    Creates a new driver.
    
    Args:
        validated_data (DriverCreateOrUpdate): The validated driver data.
        db (AsyncSession): The database session.
    
    Returns:
        ORJSONResponse: The response indicating the result of the creation.
    """
    try:
        # Asynchronously check if the username already exists
        result = await db.execute(
            select(Driver).where(Driver.username == validated_data.username).limit(1)
        )
        if result.scalars().first():
            return ORJSONResponse(
                status_code=status.HTTP_400_BAD_REQUEST,
                content={"detail": "Nom d'utilisateur déjà utilisé."},
            )

        hashed_password = argon2_strong_hash(validated_data.password)

        new_driver = Driver(
            username=validated_data.username,
            password=hashed_password,
            store=validated_data.store,
            is_active=True,
        )

        db.add(new_driver)
        await db.commit()

        return ORJSONResponse(
            content={"detail": "Livreurs créé!"},
            status_code=status.HTTP_201_CREATED,
        )
    except Exception as e:
        await db.rollback()
        logging.warning(f"Database error in create driver: {e}")
        return ORJSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": "Erreur inconnue dans la base de données."},
        )

async def update_driver(
    validated_data: DriverCreateOrUpdate,
    db: AsyncSession,
) -> ORJSONResponse:
    """
    Updates an existing driver.
    
    Args:
        validated_data (DriverCreateOrUpdate): The validated driver data.
        db (AsyncSession): The database session.
    
    Returns:
        ORJSONResponse: The response indicating the result of the update.
    """
    try:
        result = await db.execute(
            select(Driver).where(Driver.id == validated_data.driver_id).limit(1),
        )
        driver = result.scalars().first()

        if not driver:
            return ORJSONResponse(
                status_code=status.HTTP_404_NOT_FOUND,
                content={"detail": "Livreurs non trouvé."},
            )

        driver.password = argon2_strong_hash(validated_data.password)
        driver.store = validated_data.store

        await db.commit()
        await db.refresh(driver)

        return ORJSONResponse(
            content={"detail": "Livreurs mis à jour!"},
            status_code=status.HTTP_200_OK,
        )
    except Exception as e:
        logging.warning(f"Error in update_driver: {e}")
        return ORJSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": "Une erreur inconnue s'est produite."},
        )

async def driver_status(
    validated_data: DriverId,
    db: AsyncSession,
    activate: bool = True,
) -> ORJSONResponse:
    """
    Activates or deactivates a driver.
    
    Args:
        validated_data (DriverId): The validated driver ID data.
        db (AsyncSession): The database session.
        activate (bool): Whether to activate or deactivate the driver.
    
    Returns:
        ORJSONResponse: The response indicating the result of the status change.
    """
    try:
        result = await db.execute(
            select(Driver).where(Driver.id == validated_data.driver_id).limit(1),
        )
        driver = result.scalars().first()

        # Check if the driver exists
        if not driver:
            return ORJSONResponse(
                status_code=status.HTTP_404_NOT_FOUND,
                content={"detail": "Livreur non trouvé."},
            )

        # Activate or deactivate the driver
        message_ok = "Activation" if activate else "Déactivation"
        message_not_ok = "l'activation" if activate else "la déactivation"
        driver.is_active = True if activate else False

        await db.commit()
        await db.refresh(driver)

        return ORJSONResponse(
            content={"detail": f"{message_ok} avec succès de {driver.username}."},
            status_code=status.HTTP_200_OK,
        )
    except Exception as e:
        logging.warning(f"Error in driver_status: {e}")
        return ORJSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": f"Échec lors de {message_not_ok} de {driver.username}."},
        )