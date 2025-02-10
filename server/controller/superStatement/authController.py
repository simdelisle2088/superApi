from asyncio import exceptions
import logging
import os
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from fastapi.responses import ORJSONResponse
from datetime import datetime
from argon2 import PasswordHasher, Type
from fastapi import HTTPException, status, Request
import base64
import orjson
from datetime import datetime, timedelta
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from model.superStatement.StatementModel import (
    LoginStatement,
    LoginStatementCreate,
    LoginStatementResponse,
)

from utility.util import (
    ARGON2_SECRET_KEY,
    JWE_SECRET_KEY,
    PRIVATE_KEY,
    PUBLIC_KEY,

)

# Authentication / Security Utilities
def argon2_strong_hash(password: str) -> str:

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

def create_jwe(driver_id: int) -> str:
    
    # Calculate the date one day from now
    expiration_date = datetime.utcnow() + timedelta(days=1)
    expiration_date = expiration_date.strftime("%Y%m%d")

    # Create the claims
    claims = {"sub": driver_id, "exp": expiration_date}

    # Create a JWT
    payload_b85 = base64.b85encode(orjson.dumps(claims)).decode()
    jwt = f"{payload_b85}"

    # Sign the JWT with Ed25519
    signature = PRIVATE_KEY.sign(jwt.encode())
    signature_b85 = base64.b85encode(signature).decode()
    signed_jwt = f"{jwt}.{signature_b85}"

    # Generate a random nonce
    nonce = os.urandom(12)

    # Encrypt the signed JWT
    aesgcm = AESGCM(JWE_SECRET_KEY)
    ciphertext = aesgcm.encrypt(nonce, signed_jwt.encode(), None)

    # Create the JWE object
    iv_b85 = base64.b85encode(nonce).decode()
    ciphertext_b85 = base64.b85encode(ciphertext).decode()
    jwe = f"{iv_b85}.{ciphertext_b85}"
    return jwe

def auth(request: Request, jwe: str) -> bool:
    
    try:
        iv_b85, ciphertext_b85 = jwe.split(".")
        iv = base64.b85decode(iv_b85)
        ciphertext = base64.b85decode(ciphertext_b85)
        aesgcm = AESGCM(JWE_SECRET_KEY)
        signed_jwt = aesgcm.decrypt(iv, ciphertext, None).decode()
        payload_b85, signature_b85 = signed_jwt.rsplit(".", 1)
        signature = base64.b85decode(signature_b85)
        PUBLIC_KEY.verify(signature, payload_b85.encode())
        payload = orjson.loads(base64.b85decode(payload_b85))
        expiration_date = datetime.strptime(payload["exp"], "%Y%m%d")
        if datetime.utcnow().date() >= expiration_date.date():
            return False
        request.state.driver_id = int(payload["sub"])
        return True
    except Exception as e:
        print(f"Authentication failed: {e}")
        return False

async def loginStatement(data: LoginStatementCreate, db: AsyncSession) -> ORJSONResponse:

    try:
        # Asynchronously query the database
        result = await db.execute(
            select(LoginStatement).where(
                    LoginStatement.username == data.username,
            ).limit(1)
        )
        user = result.scalars().first()
        if not user:
            logging.warning(f"No user found: {data.username}")
            return ORJSONResponse(
                status_code=status.HTTP_401_UNAUTHORIZED,
                content={"detail": "Nom d'utilisateur ou mot de passe incorrect."},
            )
        # Verify the password
        hasher = PasswordHasher()
        peppered_password = f"{data.password}{ARGON2_SECRET_KEY}"
        try:
            hasher.verify(user.password, peppered_password)
        except exceptions.VerifyMismatchError:
            logging.warning(f"Invalid password for: {data.username}")
            return ORJSONResponse(
                status_code=status.HTTP_401_UNAUTHORIZED,
                content={"detail": "Nom d'utilisateur ou mot de passe incorrect."},
            )
        token = create_jwe(user.id)
        return ORJSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "token": token
            },
        )
    except Exception as e:
        await db.rollback()
        logging.warning(f"Database error for: {data.username} - {e}")
        return ORJSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": "Erreur de la base de données."},
        )

async def create_user(data: LoginStatementCreate, db: AsyncSession) -> LoginStatementResponse:

    async with db.begin():
        result = await db.execute(
            select(LoginStatement).filter(LoginStatement.username == data.username)
        )
        existing_user = result.scalars().first()
        if existing_user:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Username already taken",
            )
        hashed_password = argon2_strong_hash(data.password)
        new_user = LoginStatement(username=data.username, password=hashed_password)
        db.add(new_user)
        await db.commit()
        return LoginStatementResponse(id=new_user.id, username=new_user.username)
