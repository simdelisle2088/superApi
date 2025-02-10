import datetime
from typing import List
from argon2 import PasswordHasher
from argon2 import PasswordHasher, Type
from fastapi.security import OAuth2PasswordBearer
import jwt
from datetime import datetime, timedelta
import pytz
from sqlalchemy import select
from sqlalchemy.orm import Session, joinedload
from fastapi import Depends, HTTPException, Security, exceptions

from model.dbModel import AllUsers, PermissionResponse, Role, RoleResponse, User, UserResponse
from utility.util import ARGON2_SECRET_KEY, JWE_SECRET_KEY, get_primary_db,AsyncSession
 
SECRET_KEY = JWE_SECRET_KEY
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_HOURS = 120

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")
EASTERN_TZ = pytz.timezone("America/New_York")

def create_jwt_token(user_id: int) -> str:
    expiration = datetime.now(EASTERN_TZ) + timedelta(hours=ACCESS_TOKEN_EXPIRE_HOURS)
    to_encode = {"sub": str(user_id), "exp": expiration}
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

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

def authenticate_user(token: str = Security(oauth2_scheme), db: Session = Depends(get_primary_db)) -> User:
    credentials_exception = HTTPException(status_code=401, detail="Could not validate credentials")
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id: str = payload.get("sub")
        if user_id is None:
            raise credentials_exception
    except jwt.PyJWTError:
        raise credentials_exception

    # Retrieve the user from the database
    user = db.query(User).filter(User.id == int(user_id)).first()
    if user is None:
        raise credentials_exception
    return user

def has_permission(user: User, permission: str):
    user_permissions = [perm.name for perm in user.role.permissions]
    if permission not in user_permissions:
        raise HTTPException(status_code=403, detail="Not enough permissions")
    
def verify_password(stored_password: str, provided_password: str) -> bool:
    peppered_password = f"{provided_password}{ARGON2_SECRET_KEY}"
    try:
        hasher = PasswordHasher()
        hasher.verify(stored_password, peppered_password)
        return True
    except exceptions.VerifyMismatchError:
        return False

# The function to create a user
async def create_user_role(db: AsyncSession, username: str, password: str, role_name: str, store: str):
    try:
        # Map the store name to its corresponding ID
        store_mapping = {
            'st-hubert': 1,
            'st-jean': 2,
            'châteauguay': 3
        }
        
        # Ensure the provided store exists in the mapping
        if store.lower() not in store_mapping:
            raise HTTPException(status_code=400, detail="Invalid store")

        # Get the store ID from the mapping
        store_id = store_mapping[store.lower()]

        # Fetch the role and eagerly load its permissions
        result = await db.execute(
            select(Role).options(joinedload(Role.permissions)).filter_by(name=role_name)
        )
        role = result.scalars().first()

        if not role:
            raise HTTPException(status_code=404, detail="Role not found")

        # Check if the username already exists
        result = await db.execute(select(User).filter_by(username=username))
        existing_user = result.scalars().first()

        if existing_user:
            raise HTTPException(status_code=400, detail="Username already exists")

        # Hash the password using argon2 (or any other method you're using)
        hashed_password = argon2_strong_hash(password)

        # Collect the permissions for the role
        permissions = [{"id": str(perm.id), "name": perm.name} for perm in role.permissions]

        # Create a new user with the hashed password, assigned role, and permissions
        new_user = User(username=username, password=hashed_password, role=role, permissions=permissions, store=store_id)
        db.add(new_user)

        # Commit the transaction
        await db.commit()

        # Refresh the instance to reflect the changes in the database
        await db.refresh(new_user)

        # Return a dictionary representation of the user
        return {
            "id": new_user.id,
            "username": new_user.username,
            "role": {
                "id": role.id,
                "name": role.name,
                "permissions": new_user.permissions
            },
            "store": new_user.store
        }

    except Exception as e:
        print(f"Error occurred: {str(e)}")
        raise HTTPException(status_code=500, detail="An internal error occurred while creating the user")

async def update_user_role(
    db: AsyncSession, user_id: int, username: str, role_name: str, store: str
):
    try:
        # Map the store name to its corresponding ID
        store_mapping = {
            'st-hubert': 1,
            'st-jean': 2,
            'châteauguay': 3
        }
        
        # Ensure the provided store exists in the mapping
        if store.lower() not in store_mapping:
            raise HTTPException(status_code=400, detail="Invalid store")

        # Get the store ID from the mapping
        store_id = store_mapping[store.lower()]

        # Fetch the user by ID
        result = await db.execute(select(User).filter_by(id=user_id).options(joinedload(User.role)))
        user = result.scalars().first()

        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        # Check if the username is already used by another user
        if user.username != username:
            result = await db.execute(select(User).filter_by(username=username))
            existing_user = result.scalars().first()
            if existing_user:
                raise HTTPException(status_code=400, detail="Username already exists")

        # Fetch the role and eagerly load its permissions
        result = await db.execute(
            select(Role).options(joinedload(Role.permissions)).filter_by(name=role_name)
        )
        role = result.scalars().first()

        if not role:
            raise HTTPException(status_code=404, detail="Role not found")

        # Update the user attributes
        user.username = username
        user.role = role
        user.store = store_id

        # Automatically update the user's permissions based on the new role
        user.permissions = [{'id': perm.id, 'name': perm.name} for perm in role.permissions]  # Only serialize necessary fields

        # Commit the transaction
        await db.commit()

        # Refresh the user instance to reflect the changes in the database
        await db.refresh(user)

        # Return a dictionary representation of the updated user
        return {
            "id": user.id,
            "username": user.username,
            "role": {
                "id": role.id,
                "name": role.name,
                "permissions": user.permissions  # Return updated permissions
            },
            "store": user.store
        }

    except Exception as e:
        print(f"Error occurred: {str(e)}")
        raise HTTPException(status_code=500, detail="An internal error occurred while updating the user")


async def get_all_users(db: AsyncSession) -> List[AllUsers]:
    result = await db.execute(
        select(User)
        .options(
            joinedload(User.role).joinedload(Role.permissions) 
        )
    )

    users = result.unique().scalars().all()  

    user_responses = []

    for user in users:
        permissions = [
            PermissionResponse(id=perm.id, name=perm.name)
            for perm in user.role.permissions
        ]
        
        role_response = RoleResponse(
            id=user.role.id,
            name=user.role.name,
            permissions=permissions
        )

        user_response = AllUsers(
            id=user.id,
            username=user.username,
            role=role_response,
            permissions=permissions,
            store=user.store
        )

        user_responses.append(user_response)

    return user_responses