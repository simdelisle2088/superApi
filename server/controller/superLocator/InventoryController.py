from sqlalchemy import func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from model.superLocator.InvModel import InvLocations

async def get_items_by_full_location(full_location: str, session: AsyncSession):
    query = (
        select(
            InvLocations.name,
            InvLocations.upc,
            func.count(InvLocations.name).label('count')
        )
        .where(
            InvLocations.full_location == full_location,
            InvLocations.is_archived == False
        )
        .group_by(
            InvLocations.name,
            InvLocations.upc
        )
    )
    
    result = await session.execute(query)
    items = result.fetchall()
    return [{"name": item.name, "upc": item.upc, "count": item.count} for item in items]
