from typing import List
from pydantic import BaseModel
from pyrate_limiter import Optional
from sqlalchemy import JSON, Column, Integer, String, ForeignKey, Table
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class CreateUserRequest(BaseModel):
    username: str
    password: str
    role_name: str
    store: str

class UserCreate(BaseModel):
    username: str
    password: str
    role_name: str  # Instead of role_id, we'll pass the role name

# Permission response model
class PermissionResponse(BaseModel):
    id: int
    name: str

    class Config:
        orm_mode = True

# Role response model
class RoleResponse(BaseModel):
    id: int
    name: str
    permissions: Optional[List[PermissionResponse]] = []  # Include the permissions in the RoleResponse

    class Config:
        orm_mode = True

# User response model
class UserResponse(BaseModel):
    id: int
    username: str
    role: RoleResponse  # Include the role with permissions

    class Config:
        orm_mode = True

class LoginRequest(BaseModel):
    username: str
    password: str

class LoginResponse(BaseModel):
    id: int
    username: str
    role: RoleResponse
    access_token: str
    store: int

class AllUsers(BaseModel):
    id: int
    username: str
    store: int
    role: RoleResponse

class UpdateUserRequest(BaseModel):
    username: str
    role_name: str
    store: str

# Many-to-Many relationship between Role and Permission
role_permission = Table(
    'role_permission', Base.metadata,
    Column('role_id', ForeignKey('roles.id'), primary_key=True),
    Column('permission_id', ForeignKey('permissions.id'), primary_key=True)
)

class Role(Base):
    __tablename__ = 'roles'
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)
    users = relationship('User', back_populates='role') 
    permissions = relationship('Permission', secondary=role_permission, back_populates='roles')  # This should map correctly to permissions

class Permission(Base):
    __tablename__ = 'permissions'
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)
    roles = relationship('Role', secondary=role_permission, back_populates='permissions')

class User(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String, unique=True, nullable=False)
    password = Column(String, nullable=False)
    role_id = Column(Integer, ForeignKey('roles.id'), nullable=False)
    role = relationship('Role', back_populates='users')
    permissions = Column(JSON) 
    store = Column(Integer, nullable=False)

Role.users = relationship('User', back_populates='role')
