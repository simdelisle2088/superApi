from asyncio.log import logger
import csv
import os
import sys
from typing import Dict, List, Optional
from fastapi.concurrency import asynccontextmanager, run_in_threadpool
from pydantic import BaseModel, ConfigDict
from pyrate_limiter import Duration, Rate, Limiter, Union
from fastapi.responses import ORJSONResponse, Response
from fastapi.middleware.cors import CORSMiddleware
import logging
import hmac
from fastapi.staticfiles import StaticFiles
import pytz
from sqlalchemy.future import select
from sqlalchemy.orm import joinedload
from sqlalchemy import Integer, and_, cast, func, or_, text, update
import tracemalloc
# from controller.OfflineInv import EmailConfig, LocationExporter, StoreInvRequest
# from controller.ScheduledLocationExporter import setup_scheduled_exports

from controller.Etiquette.PrixEtiquetteController import price_label_scheduled_job, qty_label_scheduled_job

from controller.superDispatch import CommisController, DispatchController
from controller.superDispatch import AuthenticationController
from controller.superDispatch.AuthenticationController import authenticate_user, create_jwt_token, create_user_role, has_permission, update_user_role, verify_password
from controller.superLocator.InventoryController import get_items_by_full_location
from controller.superStatement import (
    StatementController,
    authController
)
from model.dbModel import AllUsers, CreateUserRequest, LoginRequest, LoginResponse, PermissionResponse, Role, RoleResponse, UpdateUserRequest, User, UserCreate, UserResponse
from model.posArcModel import ClientAddressResponse, OrderRequest, PosArcDHead, PosArcDHeadBaseModel, UpdateClientRequest
from model.superDeliver.OrderModel import Order
from model.superDispatch.commisModel import Commis
from model.superLocator.ItemModel import (
    ArchiveRequest,
    BulkReturnRequest,
    ItemAllLocalisation,
    ItemArchived,
    Items,
    ItemsReturns,
    NewOrderRequest,
    OrderForm,
    ReturnResponse,
    StoreRequest
)
from model.superLocator.InvModel import (
    InvLocations
)
from model.superStatement.StatementModel import (
    ClientStatementBillCreate,
    ClientStatementBillResponse,
    EmailData,
    LoginStatementResponse,
    LoginStatementCreate
)
from model.superStatement.CustomerModel import (
    CustomerInfo, 
    CustomerInfoModel, 
    CustomerInfoResponse
)
from model.superDeliver import (
    DriverModel, 
    DriverOrderModel, 
    TentativeModel
)
from model.superStatement.TemplatesModel import (
    HTMLContent 
)
from controller.superLocator import LocatorController, PickerController
from controller.superDeliver import DriverController, DriverOrderController, GeoController, ImageController
from controller.superLocator import LoginController
from model.superLocator import InvModel, ItemModel
from model import (LoggingModel, QueryModel)
from model.superDeliver.GeolocationModel import (StoreCoordinates)
from model.superLocator import (LoginModel)
from controller.superStatement import EmailController
from controller.superTransfer import XferLoginController
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from model.superTransfer import XferLoginModel
from model.superTransfer.XferItemModel import TransferArchiveRequest, TransferCustomerId, TransferItemSchema, TransferStoreId
from utility.util import (
    BASE_PATHS_CSV,
    BASE_PATHS_DATA,
    BASE_PATHS_IMG,
    PrimarySessionLocal,
    get_primary_db,
    get_secondary_db,
    AsyncSession,
    APP_ADMIN_KEY,
    DISPATCH_ADMIN_KEY,
    STATE_OF_DEPLOYMENT,
)
from fastapi import (
    Body,
    FastAPI,
    Path,
    Query,
    Request,
    Depends,
    File,
    UploadFile,
    status,
    APIRouter,
    HTTPException,
)

# Initialize the Limiter
login_limiter = Limiter(Rate(96, Duration.DAY))

scheduler = AsyncIOScheduler(timezone="America/New_York")

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: start scheduler
    scheduler.start()
    yield
    # Shutdown: shut down scheduler
    scheduler.shutdown()

# Declare the FastAPI app
app = FastAPI(
    debug=False,
    docs_url=None,
    redoc_url=None,
    lifespan=lifespan
)

# Log the app start and print the state of deployment
logging.warning(
    f"|- ({STATE_OF_DEPLOYMENT}) ============================================== ({STATE_OF_DEPLOYMENT}) -|"
)
logging.warning(
    f"|- ({STATE_OF_DEPLOYMENT}) |-------- NOTICE: Api is now running --------| ({STATE_OF_DEPLOYMENT}) -|"
)
logging.warning(
    f"|- ({STATE_OF_DEPLOYMENT}) ============================================== ({STATE_OF_DEPLOYMENT}) -|"
)
tracemalloc.start()

# Set the event loop policy for Windows
if sys.platform == "win32":
    import asyncio
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# =============================== CRON  JOBS ===================================#
# scheduler.add_job(
#     # Run once daily at 20:00 (8 PM)
#     price_label_scheduled_job,
#     trigger=CronTrigger(minute="0", hour="20"), 
#     id="tag_prices",
#     name="Tag Prices",
#     replace_existing=True
# )
# scheduler.add_job(
#     # Run every 30 minutes from 7 AM to 7 PM
#     qty_label_scheduled_job,
#     trigger=CronTrigger(minute="*/30", hour="7-19"), 
#     id="tag_quantities",
#     name="Tag Quantities",
#     replace_existing=True
# )
# =============================== Middleware ===================================#

def authenticate(request: Request):
    if not DriverController.authenticate(
        request, request.headers.get("X-Deliver-Auth")
    ):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Non autorisé"
        )
    
def auth_picker(request: Request):
    if not LoginController.authenticate(
        request, request.headers.get("X-Picker-Auth")
    ):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Non autorisé"
        )

def is_app_admin(request: Request):
    if not hmac.compare_digest(request.headers.get("X-Admin-Key"), APP_ADMIN_KEY):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Non autorisé"
        )


def is_dispatch(request: Request):
    if not hmac.compare_digest(
        request.headers.get("X-Dispatch-Key"), DISPATCH_ADMIN_KEY
    ):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Non autorisé"
        )

async def get_driver_id(request: Request):
    driver_id = request.headers.get("X-Driver-Id")
    if not driver_id:
        raise HTTPException(status_code=400, detail="Driver ID not provided")
    request.state.driver_id = driver_id

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount(
    "/images",
    StaticFiles(directory=BASE_PATHS_IMG.get(STATE_OF_DEPLOYMENT)),
    name="images",
)

app.mount(
    "/ftp-user",
    StaticFiles(directory=BASE_PATHS_CSV.get(STATE_OF_DEPLOYMENT)),
    name="ftp-user",
)

app.mount(
    "/data-user",
    StaticFiles(directory=BASE_PATHS_DATA.get(STATE_OF_DEPLOYMENT)),
    name="data-user",
)

# Declare the specific APIRouters with middleware
register_router = APIRouter(
    prefix="/register",
)
driver_router = APIRouter(
    prefix="/driver",
    dependencies=[Depends(is_dispatch)],
)
driver_order_router = APIRouter(
    prefix="/driver_order",
    dependencies=[Depends(authenticate)],
)
driver_transfer_router = APIRouter(
    prefix="/driver_transfer",
    dependencies=[Depends(authenticate)],
)
geo_router = APIRouter(
    prefix="/geo",
    dependencies=[Depends(authenticate)],
)
image_router = APIRouter(
    prefix="/save_images",
    dependencies=[Depends(authenticate)],
)
admin_router = APIRouter(
    prefix="/admin",
    dependencies=[Depends(authenticate), Depends(is_app_admin)],
)
dispatch_router = APIRouter(
    prefix="/misc_dispatch",
    dependencies=[Depends(is_dispatch)],
)
statement_router = APIRouter(
    prefix="/statement",
    dependencies=[Depends(is_dispatch)],
)

# async def scheduled_commis_update():
#     async with PrimarySessionLocal() as db:
#         try:
#             await CommisController.process_commis_xlsx(db)
#         except Exception as e:
#             print(f"Error in scheduled update: {e}")

# scheduler.add_job(
#     scheduled_commis_update,
#     "cron",
#     minute="*/15",  # Run every 15 minutes
#     misfire_grace_time=3600  # 1-hour grace period if the job fails to run
# )

# @app.on_event("startup")
# async def start_scheduler():
#     if not scheduler.running:
#         scheduler.start()
#     print("Scheduler started with daily job")

# ======================== Store Coords ======================================#
store_coordinates = {
    1: '45.48750046461106,-73.38457638559589',
    2: '45.33034882948999,-73.29479794494063',
    3: '45.35040656602404,-73.68937198884842',
}

@app.get("/store_coordinates", response_model=StoreCoordinates)
async def get_store_coordinates():
    return {"coordinates": store_coordinates}

# =========================SUPER DELIVER =====================================#

@register_router.post("/login")
async def login(
    data: DriverModel.DriverLogin,
    db: AsyncSession = Depends(get_primary_db),
) -> ORJSONResponse:
    return await DriverController.login(data, db)

@driver_router.post("/get_drivers")
async def get_drivers(
    data: DriverModel.StoreId,
    db: AsyncSession = Depends(get_primary_db),
) -> ORJSONResponse:
    return await DriverController.get_all_drivers(data, db)

@driver_router.post("/get_orders_per_day")
async def get_orders_per_day(
    data: DriverModel.StoreId,
    db: AsyncSession = Depends(get_primary_db),
) -> ORJSONResponse:
    return await DriverController.get_order_count_per_day(data, db)

@driver_router.post("/get_driver")
async def get_driver(
    data: DriverModel.DriverId,
    db: AsyncSession = Depends(get_primary_db),
) -> ORJSONResponse:
    return await DriverController.get_driver(data, db)


@driver_router.post("/create_driver")
async def create_driver(
    data: DriverModel.DriverCreateOrUpdate,
    db: AsyncSession = Depends(get_primary_db),
) -> ORJSONResponse:
    return await DriverController.create_driver(data, db)


@driver_router.post("/update_driver")
async def update_driver(
    data: DriverModel.DriverCreateOrUpdate,
    db: AsyncSession = Depends(get_primary_db),
) -> ORJSONResponse:
    return await DriverController.update_driver(data, db)


@driver_router.post("/activate")
async def activate_driver(
    data: DriverModel.DriverId,
    db: AsyncSession = Depends(get_primary_db),
) -> ORJSONResponse:
    return await DriverController.driver_status(data, db, activate=True)


@driver_router.post("/deactivate")
async def deactivate_driver(
    data: DriverModel.DriverId,
    db: AsyncSession = Depends(get_primary_db),
) -> ORJSONResponse:
    return await DriverController.driver_status(data, db, activate=False)

@driver_order_router.get("/get_routes_order")
async def get_routes_order(
    request: Request,
    db: AsyncSession = Depends(get_primary_db),
) -> ORJSONResponse:
    return await DriverOrderController.get_routes_order(
        request.state.driver_id, db
    )

@driver_order_router.get("/get_driver_orders")
async def get_driver_orders(
    request: Request,
    db: AsyncSession = Depends(get_primary_db),
) -> ORJSONResponse:
    return await DriverOrderController.get_driver_orders(request.state.driver_id, db)

@driver_order_router.post("/get_latest_route", status_code=status.HTTP_200_OK)
async def get_latest_route(
    data: DriverModel.DriverId,
    session: AsyncSession = Depends(get_primary_db)
) -> ORJSONResponse:
    try:
        # Query to get the latest route for the given driver_id
        query = (
            select(DriverOrderModel.DriverOrder.route)
            .where(DriverOrderModel.DriverOrder.driver_id == data.driver_id, 
                   DriverOrderModel.DriverOrder.is_delivered == True)
            .order_by(
                cast(
                    func.substring_index(DriverOrderModel.DriverOrder.route, '-', -1), Integer
                ).desc()
            )
            .limit(1)
        )
        
        result = await session.execute(query)
        latest_route = result.scalar()

        if latest_route is None:
            latest_route_number = 0
        else:
            # Extract the route number part from the format "driver_id-<route_number>"
            try:
                latest_route_number = int(latest_route.split("-")[1])
            except (ValueError, IndexError):
                latest_route_number = 0  # Fallback if parsing fails

        return ORJSONResponse({"latest_route": latest_route_number})
    
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

@driver_order_router.post("/reorder_driver_orders")
async def reorder_driver_orders(
    request: Request,
    data: List[DriverOrderModel.OrderReorderRequest],
    db: AsyncSession = Depends(get_primary_db),
) -> ORJSONResponse:
    driver_id = request.state.driver_id
    return await DriverOrderController.reorder_driver_orders(driver_id, data, db)

@driver_order_router.post("/set_driver_order")
async def set_driver_order(
    request: Request,
    data: DriverOrderModel.InvoiceCode,
    db: AsyncSession = Depends(get_primary_db),
) -> ORJSONResponse:
    # Extract the route from the request body
    body = await request.json()
    route = body.get('route')
    received_by = body.get('received_by')

    return await DriverOrderController.set_driver_order(
        request.state.driver_id, data, db, route, received_by
    )

@driver_order_router.post("/received_by")
async def received_by(
    data: DriverOrderModel.ReceivedBy,
    db: AsyncSession = Depends(get_primary_db),
) -> ORJSONResponse:
    response = await DriverOrderController.received_by(data, db)
    return ORJSONResponse(content=response)

@driver_order_router.post("/set_to_arrived")
async def set_to_arrived(
    data: DriverOrderModel.InvoiceInfo,
    db: AsyncSession = Depends(get_primary_db),
) -> ORJSONResponse:
    return await DriverOrderController.set_to_arrived(data, db)

@driver_order_router.post("/set_to_delivered")
async def set_to_delivered(
    data: DriverOrderModel.InvoiceInfo,
    db: AsyncSession = Depends(get_primary_db),
) -> ORJSONResponse:
    return await DriverOrderController.set_to_delivered(data, db)

@driver_order_router.post("/scan_part")
async def scan_part(
    data: DriverOrderModel.ScannedPart,
    db: AsyncSession = Depends(get_primary_db),
) -> ORJSONResponse:
    return await DriverOrderController.scan_part(
        data, db, scan_type="num_scanned"
    )

@driver_order_router.post("/scan_part_confirmed")
async def scan_part_confirmed(
    data: DriverOrderModel.ScannedPart,
    db: AsyncSession = Depends(get_primary_db),
) -> ORJSONResponse:
    return await DriverOrderController.scan_part(
        data, db, scan_type="confirmed_scanned"
    )

@driver_order_router.post("/batch_scan")
async def batch_scan(
    request: Request,
    data: DriverOrderModel.ScannedBatch,
    db: AsyncSession = Depends(get_primary_db),
) -> ORJSONResponse:
    return await DriverOrderController.batch_scan(request.state.driver_id, data, db)

@driver_order_router.post("/skip_part_at_delivery")
async def skip_part_at_delivery(
    request: Request,
    data: DriverOrderModel.ScannedBatch,
    db: AsyncSession = Depends(get_primary_db),
) -> ORJSONResponse:
    return await DriverOrderController.skip_part_at_delivery(
        request.state.driver_id, data, db
    )

@driver_order_router.post("/remove_active_driver_order")
async def remove_active_driver_order(
    data: DriverOrderModel.InvoiceInfo,
    db: AsyncSession = Depends(get_primary_db),
) -> ORJSONResponse:
    return await DriverOrderController.remove_driver_order(data, db, is_dispatch=False)

@driver_order_router.post("/retour_driver_orders")
async def retour_driver_orders(
    data: TentativeModel.TentativeOrder,
    db: AsyncSession = Depends(get_primary_db),
) -> ORJSONResponse:
    return await DriverOrderController.retour_driver_orders(data, db, is_dispatch=False)

@driver_order_router.post("/cancel_driver_orders")
async def cancel_driver_orders(
    data: TentativeModel.TentativeOrder,
    db: AsyncSession = Depends(get_primary_db),
) -> ORJSONResponse:
    return await DriverOrderController.cancel_driver_orders(data, db, is_dispatch=False)

@driver_order_router.post("/start_route", status_code=status.HTTP_200_OK)
async def start_route(request: DriverOrderModel.StartRouteRequest, session: AsyncSession = Depends(get_primary_db)):
    try:
        # Update all orders in the route to set route_started to True
        stmt = update(DriverOrderModel.DriverOrder).where(DriverOrderModel.DriverOrder.route == request.route).values(route_started=True)
        await session.execute(stmt)
        await session.commit()

        return {"detail": "Route started successfully"}
    except Exception as e:
        await session.rollback()
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

@driver_order_router.post("/update_route_started", status_code=status.HTTP_200_OK)
async def update_route_started(request: DriverOrderModel.StartRouteRequest, session: AsyncSession = Depends(get_primary_db)):
    try:
        # Update all orders in the route to set route_started to True
        stmt = update(DriverOrderModel.DriverOrder).where(DriverOrderModel.DriverOrder.route == request.route).values(route_started=False)
        await session.execute(stmt)
        await session.commit()

        return {"detail": "Route canceled successfully"}
    except Exception as e:
        await session.rollback()
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

@geo_router.post("/set_driver_telemetry")
async def set_driver_telemetry(
    request: Request,
    data: LoggingModel.TelemetryRequest,
    db: AsyncSession = Depends(get_primary_db),
) -> ORJSONResponse:
    return await GeoController.set_driver_telemetry(request.state.driver_id, data, db)

@image_router.post("/photo")
async def photos(
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_primary_db),
) -> ORJSONResponse:
    return await ImageController.store_image(file, db, type="photos")

@dispatch_router.post("/get_all_deliveries_time")
async def get_deliveries_time(
    data: DriverModel.StoreId,
    db: AsyncSession = Depends(get_primary_db),
) -> Response:
    return await DriverOrderController.get_all_deliveries_time(
        data.store, db
    )

@dispatch_router.post("/get_deliveries_time")
async def get_deliveries_time(
    data: DriverModel.DeliveryTimeRequest,
    db: AsyncSession = Depends(get_primary_db),
) -> Response:
    return await DriverOrderController.get_deliveries_time(
        data.store_id, data.driver_id, db
    )

@dispatch_router.post("/get_all_location")
async def get_all_location(
    data: DriverModel.StoreId,
    params: Optional[QueryModel.QueryParams] = None,
    db_primary: AsyncSession = Depends(get_primary_db),
) -> Response:
    return await LocatorController.get_all_location(data.store, params, db_primary)

@dispatch_router.post("/get_all_orders_by_id")
async def get_all_orders_by_id(
    data: DriverModel.DriverId,
    db: AsyncSession = Depends(get_primary_db),
    db_secondary: AsyncSession = Depends(get_secondary_db),
) -> Response:
    return await DriverOrderController.get_all_orders_by_id(data, db,db_secondary)

@dispatch_router.post("/get_all_routes_by_driverId")
async def get_all_routes_by_driverId(
    data: DriverModel.DriverIdRoute,
    db: AsyncSession = Depends(get_primary_db),
) -> Response:
    return await DriverOrderController.get_all_routes_by_driverId(data, db)

@dispatch_router.post("/remove_any_driver_order")
async def remove_any_driver_order(
    data: DriverOrderModel.InvoiceInfo,
    db: AsyncSession = Depends(get_primary_db),
) -> ORJSONResponse:
    return await DriverOrderController.remove_driver_order(data, db, is_dispatch=True)

@dispatch_router.post("/remove_cancel_order")
async def remove_cancel_order(
    data: DriverOrderModel.InvoiceInfo,
    db: AsyncSession = Depends(get_primary_db),
) -> ORJSONResponse:
    return await DriverOrderController.remove_cancel_order(data, db)

@dispatch_router.get("/get_route")
async def get_route_endpoint(
    origin: str = Query(..., description="Origin coordinates (latitude,longitude)"),
    destination: str = Query(..., description="Destination coordinates (latitude,longitude)"),
    departureTime: str = Query(None, description="Optional departure time"),
) -> ORJSONResponse:
    response = await GeoController.get_route(origin, destination, departureTime)
    return ORJSONResponse(content=response)

@dispatch_router.get("/get_route_info")
async def get_route_endpoint(
    routes: List[str] = Query(..., description="List of group index"),
    db: AsyncSession = Depends(get_primary_db),
) -> ORJSONResponse:
    response = await GeoController.get_route_info(routes, db)
    return ORJSONResponse(content=response)

@dispatch_router.post("/get_oauth_token")
async def get_oauth_token_endpoint():
    token_data = await run_in_threadpool(GeoController.get_oauth_token)  # Run the synchronous function in a thread pool
    return ORJSONResponse(content=token_data)


# ============================END SUPER DELIVER =====================================================#

# ============================SUPER LOCATOR =====================================================#

@app.post("/locator/get_new_order")
async def check_new_orders(
    data: dict,
    db: AsyncSession = Depends(get_primary_db),
) -> ORJSONResponse:
    store_id = data.get('store_id')
    level = data.get('level')
    return await LocatorController.check_new_orders(store_id, level, db)

@app.post("/picker/get_picking_orders/{user_id}")
async def get_picking_orders(
    data: ItemModel.StoreId,  # Request body should come first
    user_id: int = Path(..., description="The ID of the logged-in user"),  # Path parameter after
    db: AsyncSession = Depends(get_primary_db),  # Dependencies last
) -> ORJSONResponse:
    try:
        user_query = select(LoginModel.UsersLocator).where(LoginModel.UsersLocator.id == user_id)
        result = await db.execute(user_query)
        user = result.scalar_one_or_none()
        
        if not user:
            return ORJSONResponse(
                status_code=status.HTTP_404_NOT_FOUND,
                content={"detail": "User not found"},
            )
            
        if user.store != data.store:
            return ORJSONResponse(
                status_code=status.HTTP_403_FORBIDDEN,
                content={"detail": "User not authorized for this store"},
            )
            
        return await PickerController.get_picking_orders(data, db, user_id)
        
    except Exception as e:
        logging.error(f"Error processing request: {e}", exc_info=True)
        return ORJSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": "An error occurred processing your request"},
        )
    
@app.get('/picker/get_all_missing_items')
async def get_all_missing_items(
    db: AsyncSession = Depends(get_primary_db),
) -> ORJSONResponse:
    return await PickerController.get_all_missing_items(db)

@dispatch_router.post("/create_locator")
async def create_locator(
    data: LoginModel.LocatorCreate,
    db: AsyncSession = Depends(get_primary_db),
) -> ORJSONResponse:
    return await LoginController.create_locator(data, db)


@register_router.post("/login_locator")
async def login_locator(
    data: LoginModel.LocatorLogin,
    db: AsyncSession = Depends(get_primary_db),
) -> ORJSONResponse:
    return await LoginController.login_locator(data, db)

@app.post("/locator/set_localisation")
async def set_localisation(
    data: InvModel.InvScan,
    db: AsyncSession = Depends(get_primary_db),
) -> ORJSONResponse:
    return await LocatorController.set_localisation(data, db)

@app.post("/locator/get_info")
async def get_info_by_upc(
    data: ItemModel.FindItem,
    db: AsyncSession = Depends(get_primary_db),
) -> ORJSONResponse:
    return await LocatorController.get_info_by_upc(data, db) 

@app.post("/picker/get_localisation")
async def get_localisation(
    data: ItemModel.ItemLocalisation,
    db: AsyncSession = Depends(get_primary_db),
) -> ORJSONResponse:
    return await PickerController.get_localisation(data, db) 

@app.post("/picker/get_all_localisation")
async def get_all_localisation(
    data: ItemModel.ItemAllLocalisation,
    db: AsyncSession = Depends(get_primary_db),
) -> ORJSONResponse:
    return await PickerController.get_all_localisation(data, db) 

@app.post("/picker/get_user_by_id")
async def get_user_by_id(
    data: LoginModel.UserId,
    db: AsyncSession = Depends(get_primary_db),
) -> ORJSONResponse:
    return await PickerController.get_user_by_id(data, db) 

class ReserveItemRequest(BaseModel):
    data: ItemModel.ItemReserved
    user_id: int

@app.post("/picker/set_to_reserved")
async def set_to_reserved(
    request: ReserveItemRequest,
    db: AsyncSession = Depends(get_primary_db),
) -> ORJSONResponse:
    return await PickerController.set_to_reserved(request.data, request.user_id, db)

@app.post("/picker/unreserved")
async def unreserved(
    data: ItemModel.ItemUpdate,
    db: AsyncSession = Depends(get_primary_db),
) -> ORJSONResponse:
    return await PickerController.update_item_status(data, db, 'is_reserved', False)

@app.post("/picker/is_missing")
async def is_missing(
    data: ItemModel.ItemUpdate,
    db: AsyncSession = Depends(get_primary_db),
) -> ORJSONResponse:
    return await PickerController.update_item_status(data, db, 'is_missing', True)

@app.post("/picker/in_stock")
async def in_stock(
    data: ItemModel.ItemUpdate,
    db: AsyncSession = Depends(get_primary_db),
) -> ORJSONResponse:
    return await PickerController.update_item_status(data, db, 'is_missing', False)

@app.post("/picker/get_upc")
async def get_upc(
    data: ItemModel.ItemLocalisation,
    db: AsyncSession = Depends(get_primary_db),
) -> ORJSONResponse:
    return await PickerController.get_upc(data, db)

@app.post("/picker/find_item_by_upc")
async def find_item_by_upc(
    data: ItemModel.upc,
    db: AsyncSession = Depends(get_primary_db),
) -> ORJSONResponse:
    return await PickerController.find_item_by_upc(data, db)

@app.post("/picker/pick_item_by_upc")
async def pick_item_by_upc(
    data: ItemModel.ItemPicked,
    db: AsyncSession = Depends(get_primary_db),
) -> ORJSONResponse:
    return await PickerController.pick_item_by_upc(data, db)

@app.post("/picker/by_pass_item_scan")
async def by_pass_item_scan(
    data: ItemModel.ByPassItem,
    db: AsyncSession = Depends(get_primary_db),
) -> ORJSONResponse:
    return await PickerController.by_pass_item_scan(data, db)

@app.post("/picker/upclocations")
async def fetch_product_locations(
    upc_request: InvModel.UPCRequest, db: AsyncSession = Depends(get_primary_db)):
    # Call the async helper function to get the full_locations
    full_locations = await LocatorController.get_locations_by_upc(upc_request.upc, db)
    return {"upc": upc_request.upc, "full_locations": full_locations}

@app.post("/inv/items_by_location", response_model=List[InvModel.ItemResponse])
async def fetch_items_by_location(
    request: InvModel.FullLocationRequest,
    session: AsyncSession = Depends(get_primary_db)
):
    items = await get_items_by_full_location(request.full_location, session)
    if not items:
        raise HTTPException(status_code=404, detail="No items found for the given full_location.")
    return items
# ============================END SUPER LOCATOR =====================================================#
# ============================ SUPER TRANSFER =====================================================#

@register_router.post("/login_xfer")
async def login_xfer(
    data: XferLoginModel.XferLogin,
    db: AsyncSession = Depends(get_primary_db),
) -> ORJSONResponse:
    return await XferLoginController.login_xfer(data, db)

@dispatch_router.post("/create_xfer")
async def create_locator(
    data: XferLoginModel.XferCreate,
    db: AsyncSession = Depends(get_primary_db),
) -> ORJSONResponse:
    return await XferLoginController.create_xfer(data, db)

@driver_transfer_router.post("/transfers", response_model=List[TransferItemSchema])
async def fetch_transfers(store_request: TransferStoreId, db: AsyncSession = Depends(get_primary_db)):
    if not store_request.store:
        raise HTTPException(status_code=400, detail="Store ID is required.")
    
    # Await the async function
    transfer_items = await XferLoginController.get_transfer_items_by_store(db, store_request.store)
    if not transfer_items:
        raise HTTPException(status_code=404, detail="No transfer items found for the specified store.")
    return transfer_items

@driver_transfer_router.get("/transfers/categories", response_model=Dict[str, List[TransferItemSchema]])
async def fetch_categorized_transfers(db: AsyncSession = Depends(get_primary_db)):
    categorized_transfers = await XferLoginController.get_categorized_transfers(db)
    if not categorized_transfers:
        raise HTTPException(status_code=404, detail="No transfers found.")
    return categorized_transfers

@driver_transfer_router.post("/transfers/customer_items", response_model=List[TransferItemSchema])
async def fetch_customer_transfers(request: TransferCustomerId, db: AsyncSession = Depends(get_primary_db)):
    transfers = await XferLoginController.get_transfers_by_customer(db, request.customer)
    
    if not transfers:
        raise HTTPException(status_code=404, detail="No transfers found for the specified customer.")
    
    return transfers

@driver_transfer_router.post("/transfers/archive_item")
async def archive_item(request: TransferArchiveRequest, db: AsyncSession = Depends(get_primary_db)):
    """
    Archives a transfer item by marking it as archived based on the provided UPC and customer.
    """
    success = await XferLoginController.archive_transfer_item(db, request.customer, request.upc)
    
    if not success:
        raise HTTPException(status_code=404, detail="Transfer item not found or already archived.")
    
    return {"message": "Item successfully archived."}

@driver_transfer_router.post("/transfers/bypass_batch_scan")
async def bypass_batch_scan(request: TransferArchiveRequest, db: AsyncSession = Depends(get_primary_db)):
    """
    Bypasses the batch scan process, directly archiving the item by setting qty_selling_units to 0.
    """
    success = await XferLoginController.bypass_batch_scan(db, request.customer, request.upc)
    if not success:
        raise HTTPException(status_code=404, detail="Transfer item not found or already archived.")

    return {"message": "Item successfully archived via bypass batch scan."}

# ============================END SUPER LOCATOR =====================================================#
# ============================SUPER STATEMENT =====================================================#

@register_router.post("/loginStatement", response_model=LoginStatementResponse)
async def loginStatement(
    data: LoginStatementCreate,
    db: AsyncSession = Depends(get_primary_db),
) -> ORJSONResponse:

    return await authController.loginStatement(data, db)

@statement_router.post("/create_user", response_model=LoginStatementResponse)
async def create_user(
    data: LoginStatementCreate,
    db: AsyncSession = Depends(get_primary_db),
) -> ORJSONResponse:
    return await authController.create_user(data, db)

@statement_router.get("/list_statements", response_model=List[ClientStatementBillResponse])
async def list_statements(
    db: AsyncSession = Depends(get_primary_db),
) -> ORJSONResponse:
    try:
        async with db as session:
            statements_response = await StatementController.fetch_statements(session)
        return ORJSONResponse(
            status_code=status.HTTP_200_OK,
            content={"statements": statements_response}
        )
    except Exception as e:
        logging.warning(f"Database error: {e}")
        return ORJSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": "Database error."}
        )

@statement_router.post("/create_client", response_model=CustomerInfoResponse)
async def create_client(
    data: CustomerInfoModel,
    db: AsyncSession = Depends(get_primary_db),
) -> ORJSONResponse:
    return await StatementController.create_client(data, db)

@statement_router.put("/update_client/{customer_number}", response_model=CustomerInfoResponse)
async def update_client(
    customer_number: str,
    data: CustomerInfoModel,
    db: AsyncSession = Depends(get_primary_db),
) -> CustomerInfoResponse:
    return await StatementController.update_client(customer_number, data, db)

@statement_router.get("/get_client/{customer_number}", response_model=CustomerInfoResponse)
async def get_client(customer_number: str, db: AsyncSession = Depends(get_primary_db)):
    async with db.begin():
        result = await db.execute(
            select(CustomerInfo).filter(CustomerInfo.customer_number == customer_number)
        )
        client = result.scalars().first()
        if not client:
            raise HTTPException(status_code=404, detail="Client not found")
        return client
    
@app.post("/get_email", response_model=List[EmailData])
async def get_email_endpoint(
    db: AsyncSession = Depends(get_primary_db),
) -> ORJSONResponse:
    emails = await EmailController.check_email(db)
    return ORJSONResponse({"emails": emails})  
    
@app.post("/send_email", response_model=ClientStatementBillResponse)
async def send_email_endpoint(
    data: ClientStatementBillCreate,
    db: AsyncSession = Depends(get_primary_db),
) -> ORJSONResponse:
    logger.info(f"Received request data: {data}")
    await StatementController.process_send_email(data, db)
    return ORJSONResponse({"message": "Emails sent successfully"})

@app.get("/fetch-html")
async def fetch_html(fileName: str, db: AsyncSession = Depends(get_primary_db)):
    async with db as session:
        html_template = await StatementController.fetch_html_template(fileName, session)
    if html_template is None:
        raise HTTPException(status_code=404, detail="File not found")
    return {"html": html_template.content}

@app.post("/save-html")
async def save_html(content: HTMLContent, db: AsyncSession = Depends(get_primary_db)):
    async with db as session:
        return await StatementController.save_html_template(content, session)

@app.post("/run-script")
def run_script():
    try:
        StatementController.execute_customer_script()
        return {"message": "Script executed successfully"}
    except FileNotFoundError as e:
        print(f"FileNotFoundError: {str(e)}")
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        print(f"ValueError: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        print(f"Exception: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/fetch-customer-data")
def fetch_customer_data():
    try:
        customers = StatementController.fetch_customer_data_from_db()
        return customers
    except Exception as e:
        print(f"Error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    
# ============================ END SUPER STATEMENT =====================================================#

# ============================ API V2 =====================================================#

@dispatch_router.post("/v2/get_client_orders_info")
async def get_client_orders_info(
    data: DriverOrderModel.ClientOrders,
    db_primary: AsyncSession = Depends(get_primary_db),
    db_sec: AsyncSession = Depends(get_secondary_db),
) -> Response:
    return await DispatchController.get_client_orders_info(data, db_primary, db_sec)

@dispatch_router.post("/v2/delivery_stats_by_store", response_model=Dict[str, DriverOrderModel.TimeRangeStats])
async def get_delivery_stats_by_store(
    store_id: DriverOrderModel.AllOrdersByStore,
    db_primary: AsyncSession = Depends(get_primary_db),
):
    return await DispatchController.get_delivery_stats_info_by_store(
        store_id.storeId, db_primary)

@dispatch_router.post("/v2/delivery_counts_by_date_range", 
                        response_model=DriverOrderModel.DeliveryStatsResponse)
async def get_delivery_counts_by_date_range(
    request: DriverOrderModel.DeliveryCountRequest,
    db_primary: AsyncSession = Depends(get_primary_db),
    db_secondary: AsyncSession = Depends(get_secondary_db)
):
    return await DispatchController.get_delivery_counts_info_by_date_range(
        request.storeId, request.startDate, request.endDate, db_primary, db_secondary
    )

@dispatch_router.post("/v2/count_by_user")
async def get_location_counts_by_user(data: InvModel.StoreIdRequest, 
    db_primary: get_primary_db = Depends(
        get_primary_db)):
    return await DispatchController.count_locations_by_user(
        db_primary, data.store)

@dispatch_router.post("/v2/picked_count")
async def get_items_picked_count(data: ItemAllLocalisation, db_primary: AsyncSession = Depends(get_primary_db)):
    return await DispatchController.count_items_picked_by_user(db_primary, data.store)

@app.get("/v2/admin/")
def get_admin_panel(current_user: User = Depends(authenticate_user)):
    # Check if the current user has the 'create_users' permission
    has_permission(current_user, 'create_users')
    return {"message": "Admin access granted"}


@app.post("/v2/create_user_role", response_model=UserResponse, status_code=status.HTTP_201_CREATED)
async def create_user_role_endpoint(
    user_request: CreateUserRequest,
    db: AsyncSession = Depends(get_primary_db)
):
    try:
        # Create the user with role and permissions
        new_user_data = await create_user_role(db, user_request.username, user_request.password, user_request.role_name, user_request.store)

        # Return the user with role and permissions
        return UserResponse(
            id=new_user_data["id"],
            username=new_user_data["username"],
            role=RoleResponse(
                id=new_user_data["role"]["id"],
                name=new_user_data["role"]["name"],
                permissions=[PermissionResponse(id=perm["id"], name=perm["name"]) for perm in new_user_data["role"]["permissions"]]
            ),
            store=new_user_data['store']
        )
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail="An internal error occurred")

@app.post("/v2/login_user", response_model=LoginResponse, status_code=status.HTTP_200_OK)
async def login_user(
    login_request: LoginRequest,
    db: AsyncSession = Depends(get_primary_db)  # Ensure you're using AsyncSession
):
    try:
        # Query the user by username asynchronously
        # Also eager load the role and permissions with joinedload for async
        result = await db.execute(
            select(User).options(joinedload(User.role).joinedload(Role.permissions))
                        .filter_by(username=login_request.username)
        )
        user = result.scalars().first()

        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        # Verify the password using the verify_password function from AuthenticationController
        if not verify_password(user.password, login_request.password):
            raise HTTPException(status_code=401, detail="Incorrect password")

        # Create a JWT token for the user
        access_token = create_jwt_token(user.id)

        # Fetch the user's role and permissions asynchronously
        role = user.role
        permissions = [{"id": str(perm.id), "name": perm.name} for perm in role.permissions]

        # Return the user data with role, permissions, and access token
        return LoginResponse(
            id=user.id,
            username=user.username,
            role=RoleResponse(
                id=role.id,
                name=role.name,
                permissions=permissions
            ),
            access_token=access_token,
            store=user.store
        )

    except HTTPException as e:
        raise e
    except Exception as e:
        print(f"Login error: {str(e)}")
        raise HTTPException(status_code=500, detail="An internal error occurred during login")

@dispatch_router.post("/v2/get_all_localisation")
async def get_all_localisation(
    data: InvModel.Localisation,
    limit: int = Query(48, description="Number of records per page"),
    offset: int = Query(0, description="Offset for pagination"),
    search_term: Optional[str] = Query(None, description="Search term for filtering"),
    db_primary: AsyncSession = Depends(get_primary_db),
) -> Response:
    return await DispatchController.get_all_localisation(data, db_primary, limit, offset, search_term)

@app.put("/v2/archive_localisation", response_model=dict)
async def archive_localisation(
    request_data: InvModel.InvLocationArchiveRequest, 
    db: AsyncSession = Depends(get_primary_db)
):
    return await DispatchController.archive_item_localisation(request_data.upc, request_data.full_location, db)

@app.post("/v2/archive_item")
async def archive_item(
    item_data: ItemArchived,
    db: AsyncSession = Depends(get_primary_db)
):
    # Sanitize the input data
    item_name = item_data.item.strip()  # Trim any surrounding spaces
    loc = item_data.loc.strip()         # Trim any surrounding spaces

    # Execute the query with case-insensitive matching and whitespace handling
    result = await db.execute(
        select(Items).where(
            func.lower(Items.item) == func.lower(item_name),
            func.lower(Items.loc) == func.lower(loc),
            Items.is_archived == False
        )
    )
    db_item = result.scalars().first()

    # Handle the case when no item is found
    if not db_item:
        print("No matching unarchived item found") 
        raise HTTPException(status_code=404, detail="No unarchived item found with the specified criteria")

    # Update the item's archived status
    db_item.is_archived = True
    await db.commit()
    await db.refresh(db_item)

    return {"message": "Item archived successfully", "item": db_item.item, "location": db_item.loc}


async def get_all_orders_by_store(
    data: DriverOrderModel.AllOrdersByStore,
    db_primary: AsyncSession = Depends(get_primary_db),
) -> Response:
    return await DispatchController.get_all_orders_by_store(data, db_primary)

@dispatch_router.post("/v2/search_driver_order")
async def search_driver_order(
    data: DriverOrderModel.SearchOrderRequest,
    db_primary: AsyncSession = Depends(get_primary_db),
    db_secondary: AsyncSession = Depends(get_secondary_db)
) -> Response:
    return await DispatchController.search_driver_order(
        data.search_query,
        data.store, 
        db_primary,
        db_secondary
    )

@dispatch_router.post("/v2/search_driver_order")
async def search_driver_order(
    data: DriverOrderModel.SearchOrderRequest,
    db_primary: AsyncSession = Depends(get_primary_db),
    db_secondary: AsyncSession = Depends(get_secondary_db)
) -> Response:
    return await DispatchController.search_driver_order(
        data.search_query,
        data.store, 
        db_primary,
        db_secondary
    )

@dispatch_router.post("/v2/get_all_drivers_orders")
async def get_all_drivers_orders(
    data: DriverModel.StoreId,
    db_primary: AsyncSession = Depends(get_primary_db),
    db_secondary: AsyncSession = Depends(get_secondary_db),
    offset: int = Query(0, alias="offset"),    
    limit: int = Query(100, alias="limit")
) -> Response:
    print("Received data:", data)  # Add this line for debugging
    if offset < 0:
        offset = 0
    
    search_terms = data.search if hasattr(data, 'search') else []
    
    return await DispatchController.get_all_drivers_orders(
        validated_data=data,
        db_primary=db_primary,
        db_secondary=db_secondary,
        offset=offset,
        limit=limit,
        search=search_terms
    )

@dispatch_router.post("/v2/get_orders_count")
async def get_orders_count(
    data: DriverModel.StoreId,
    db_primary: AsyncSession = Depends(get_primary_db),
) -> Response:
    return await DispatchController.get_orders_count(data, db_primary)

@dispatch_router.post("/v2/driver_order_counts", response_model=List[DriverOrderModel.DriverStats])
async def get_driver_stats(store: DriverOrderModel.StoreRequest, db_primary: AsyncSession = Depends(get_primary_db)):
    return await DispatchController.get_driver_delivery_stats(db_primary, store.store)

@app.post('/v2/get_all_missing_items_v2')
async def get_all_missing_items_v2(
    data: DriverModel.StoreId,
    db_primary: AsyncSession = Depends(get_primary_db),
) -> ORJSONResponse:
    return await DispatchController.get_all_missing_items_v2(data, db_primary)

@app.post("/v2/in_stock_v2")
async def in_stock_v2(
    data: ItemModel.ItemUpdate,
    db_primary: AsyncSession = Depends(get_primary_db),
) -> ORJSONResponse:
    return await PickerController.update_item_status(data, db_primary, 'is_missing', False)


@app.get("/v2/get_all_users", response_model=List[AllUsers])
async def get_users(db_primary: AsyncSession = Depends(get_primary_db)):
    return await AuthenticationController.get_all_users(db_primary)


@app.put("/v2/update_user_role/{user_id}", response_model=UserResponse) 
async def update_user(
    user_id: int,
    request: UpdateUserRequest,
    db_primary: AsyncSession = Depends(get_primary_db)
):
    updated_user = await update_user_role(
        db=db_primary,
        user_id=user_id,
        username=request.username,
        role_name=request.role_name,
        store=request.store
    )
    return updated_user

@app.get("/v2/get_pos_arc_d_head", response_model=list[PosArcDHeadBaseModel])
async def get_pos_arc_d_head(
    limit: int = 50,
    offset: int = 0,
    search: str = "",
    db_primary: AsyncSession = Depends(get_primary_db),
):
    query = select(PosArcDHead)
    
    # Add filtering if search term is provided
    if search:
        query = query.filter(
            or_(
                PosArcDHead.customer.ilike(f"%{search}%"),  # Case-insensitive match for customer name
                PosArcDHead.customer_number.ilike(f"%{search}%"),  # Case-insensitive match for customer number
            )
        )
    
    query = query.limit(limit).offset(offset)
    result = await db_primary.execute(query)
    data = result.scalars().all()
    return data

@app.put("/v2/update_client/{client_id}", response_model=UpdateClientRequest)
async def update_client(client_id: int, client_data: UpdateClientRequest, db: AsyncSession = Depends(get_primary_db)):
    updated_client = await DispatchController.update_client_in_db(client_id, client_data, db)
    return updated_client

@app.delete("/v2/delete_client/{client_id}", status_code=204)
async def delete_client(client_id: int, db: AsyncSession = Depends(get_primary_db)):
    await DispatchController.delete_client_from_db(client_id, db)
    return {"detail": "Client deleted successfully"}

@app.get("/v2/psl")
async def get_customers_with_psl(
    page: int = 1,
    limit: int = 100,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    order_number: Optional[str] = None,
    store: Optional[int] = None,  
    db: AsyncSession = Depends(get_primary_db),
):
    try:
        offset = (page - 1) * limit
        customers, total_count = await DispatchController.fetch_customers_with_psl(
            db,
            offset=offset, 
            limit=limit, 
            start_date=start_date, 
            end_date=end_date, 
            order_number=order_number,
            store=store
        )
        return {
            "data": customers,
            "total": total_count,
            "page": page,
            "total_pages": (total_count + limit - 1) // limit
        }
    except Exception as e:
        return {"error": str(e)}

@app.post("/v2/psl/drivers")
async def get_driver_names(
    request: DriverOrderModel.OrderNumberRequest, 
    session: AsyncSession = Depends(get_primary_db)
):
    try:
        # Use IN clause with a single query
        driver_query = (
            select(DriverOrderModel.DriverOrder.order_number, 
                   DriverOrderModel.DriverOrder.driver_name)
            .where(
                and_(
                    DriverOrderModel.DriverOrder.order_number.in_(request.order_numbers),
                    DriverOrderModel.DriverOrder.driver_name.isnot(None)  
                )
            )
            .with_hint(DriverOrderModel.DriverOrder, 'FORCESEEK(IX_driver_orders_order_number)', 'mssql')
        )
        
        result = await session.execute(driver_query)
        driver_data = result.fetchall()

        # Use dictionary comprehension for better performance
        return {
            "data": [
                {"order_number": row.order_number, "driver_name": row.driver_name} 
                for row in driver_data
            ]
        }
    
    except Exception as e:
        print(f"Error fetching driver names: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch driver names")
    
@app.post("/v2/picker_form")
async def add_order_info(order_form: OrderForm,
                         db_primary: AsyncSession= Depends(get_primary_db), 
                        ):
    try:
        new_item = await DispatchController.form_order_info(
            db_primary, 
            order_form.item_name, 
            order_form.quantity, 
            order_form.store_id
        )
        return {"success": True, "data": new_item}
    except ValueError as e:
        print("ValueError: %s", str(e))
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        print("Unexpected error: %s", str(e))
        raise HTTPException(status_code=500, detail="An error occurred while processing the request.")

@app.get("/v2/commis-data", response_model=List[Commis])
async def read_commis_data(db: AsyncSession = Depends(get_primary_db)):
    commis_data = await CommisController.get_all_commis_data(db)
    return commis_data

@app.post("/v2/get_long_lat_or_create")
async def get_long_lat_or_create(
    data: dict,
    db_primary: AsyncSession = Depends(get_primary_db),
    db_secondary: AsyncSession = Depends(get_secondary_db)
):
    try:
        order_number = data.get("order_number")
        job = data.get("job", 0)

        if not order_number:
            raise HTTPException(
                status_code=400,
                detail="order_number is required"
            )

        coordinates = await DriverOrderController.fetch_or_create_coordinates(
            order_number=order_number,
            job=job,
            db_primary=db_primary,      
            db_secondary=db_secondary    
        )

        return coordinates

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )

@app.get("/v2/manual_update_price_label")
async def manual_update_prixetiquette():
    try:
        await price_label_scheduled_job()
        return {"message": "Data updated successfully"}
    except Exception as e:
        return {"error": str(e)}

@app.get("/v2/manual_update_qty_label")
async def manual_update_prixetiquette():
    try:
        await qty_label_scheduled_job()
        return {"message": "Data updated successfully"}
    except Exception as e:
        return {"error": str(e)}

@app.post("/v2/locations_bulk_delete", response_model=InvModel.LocationDeleteResponse)
async def delete_locations(
    request: InvModel.LocationDeleteRequest,
    db: AsyncSession = Depends(get_primary_db)
):
    archived_count = await LocatorController.delete_locations_by_section(
        db=db,
        store=request.store,
        level=request.level,
        row=request.row,
        side=request.side,
    )
    
    return InvModel.LocationDeleteResponse(
        locations_deleted=archived_count,
        message=f"Successfully DELETED {archived_count} locations"
    )

@app.post("/v2/bulk-returns/")
async def create_bulk_returns(
    request: BulkReturnRequest,
    db: AsyncSession = Depends(get_primary_db),
):
    if not request.items:
        raise HTTPException(status_code=400, detail="No items provided")
        
    result = await DispatchController.process_return_items(db, request.items)
    
    if not result["success"] and result["errors"]:
        raise HTTPException(
            status_code=400,
            detail={
                "message": "Failed to process any items",
                "errors": result["errors"]
            }
        )
        
    return result

@app.post("/v2/get_returns", response_model=List[ReturnResponse])
async def get_active_items(
    request: StoreRequest,
    db: AsyncSession = Depends(get_primary_db),
):
    try:
        # Query the database
        query = (
            select(ItemsReturns)
            .where(ItemsReturns.is_archived == False, ItemsReturns.store == request.store)
        )
        result = await db.execute(query)
        items = result.scalars().all()

        if not items:
            raise HTTPException(
                status_code=404, 
                detail=f"No active items found for store {request.store}."
            )

        # Add error handling for UPC conversion
        responses = []
        for item in items:
            try:
                response = ReturnResponse(
                    id=item.id,
                    store=item.store,
                    item=item.item,
                    units=item.units,
                    created_at=item.created_at.isoformat(),
                    updated_at=item.updated_at.isoformat(),
                    loc=item.loc,
                    upc=item.upc,
                )
                responses.append(response)
            except Exception as e:
                logger.error(f"Error processing item {item.id}: {str(e)}")
                # Optionally skip or handle individual item errors

        if not responses:
            raise HTTPException(
                status_code=500,
                detail="Failed to process any items"
            )

        return responses

    except Exception as e:
        logger.error("Error fetching active items", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"An error occurred while fetching active items: {str(e)}"
        )

@app.post("/v2/archive_returns_item")
async def archive_item(
    request: ArchiveRequest,
    db: AsyncSession = Depends(get_primary_db),
):
    try:
        # Using MariaDB's JSON_CONTAINS function to check if UPC exists in the JSON array
        json_contains = text("JSON_CONTAINS(upc, JSON_QUOTE(:upc), '$')")
        
        # Query for items where the requested UPC exists in the JSON array
        query = (
            select(ItemsReturns)
            .where(
                ItemsReturns.store == request.store,
                json_contains,
                ItemsReturns.is_archived == False
            )
            .order_by(ItemsReturns.id.asc())
            .params(upc=request.upc)  # Pass the UPC directly, JSON_QUOTE will handle the formatting
        )
        result = await db.execute(query)
        item = result.scalars().first()

        # If no active item found, check if it's already archived
        if not item:
            logger.warning(
                f"No active (is_archived=False) ItemsReturns record found for store={request.store}, upc={request.upc}"
            )
            already_archived_query = (
                select(ItemsReturns)
                .where(
                    ItemsReturns.store == request.store,
                    json_contains,
                    ItemsReturns.is_archived == True
                )
                .params(upc=request.upc)
            )
            already_archived_result = await db.execute(already_archived_query)
            already_archived_item = already_archived_result.scalars().first()

            if already_archived_item:
                return {
                    "message": f"Item with UPC {request.upc} in store {request.store} is already archived.",
                    "status": "already_archived",
                }
            else:
                raise HTTPException(
                    status_code=404,
                    detail=f"Item with UPC {request.upc} not found in store {request.store}."
                )

        # Rest of your existing code remains the same
        current_loc = request.loc or item.loc or "010000000"
        
        if item.units > 0:
            item.units -= 1
            
            # Archive location logic
            if current_loc != "010000000":
                location_query = (
                    select(InvLocations)
                    .where(
                        InvLocations.upc == request.upc,
                        InvLocations.store == str(request.store),
                        InvLocations.full_location == current_loc,
                        InvLocations.is_archived == False
                    )
                )
                location_result = await db.execute(location_query)
                location = location_result.scalars().first()

                if location:
                    location.is_archived = True
                else:
                    logger.warning(
                        f"No matching location found for "
                        f"UPC {request.upc} with full_location {current_loc}"
                    )

            if item.units == 0:
                item.is_archived = True
                message = (
                    "Item has been archived as units reached 0, "
                    f"and the matching location {current_loc} has been archived."
                )
            else:
                message = f"Item units reduced to {item.units}, and location {current_loc} archived."
        else:
            item.is_archived = True
            
            if current_loc != "010000000":
                location_query = (
                    select(InvLocations)
                    .where(
                        InvLocations.upc == request.upc,
                        InvLocations.store == str(request.store),
                        InvLocations.full_location == current_loc,
                        InvLocations.is_archived == False
                    )
                )
                location_result = await db.execute(location_query)
                location = location_result.scalars().first()

                if location:
                    location.is_archived = True

            message = (
                f"Item has been archived as units were 0, "
                f"and the matching location {current_loc} has been archived."
            )

        await db.commit()

        return {
            "message": message,
            "status": "success",
            "remaining_units": item.units if not item.is_archived else 0,
        }

    except Exception as e:
        logger.error("Error processing item", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="An error occurred while processing the item.",
        )






# ============================ END API V2 =====================================================#
# ============================ OFFLINE INV =====================================================#
# Email configuration with direct values
# email_config = EmailConfig(
#     sender_email="distributionautopartscanada@gmail.com",  
#     app_password="gqreujpsbregrqua", 
#     recipient_email="locations@pasuper.com"  
# )

# # Initialize the exporter
# location_exporter = LocationExporter(email_config)
# setup_scheduled_exports(app, email_config)

# @app.post("/export-locations")
# async def export_locations_endpoint(
#     request: StoreInvRequest,
#     db: AsyncSession = Depends(get_primary_db)
# ):
#     """
#     Endpoint to export store locations and send them via email
    
#     Args:
#         request: Contains store_id for the export
#         db: Database session
        
#     Returns:
#         ORJSONResponse with operation status
#     """
#     try:
#         if not request.store_id:
#             raise HTTPException(
#                 status_code=400, 
#                 detail="Store ID is required"
#             )

#         # Attempt to export and send email
#         result = await location_exporter.export_locations_by_email(request.store_id, db)

#         # Handle case where no locations are found
#         if "No locations found" in result:
#             return ORJSONResponse(
#                 status_code=404,
#                 content={
#                     "message": result,
#                     "status": "not_found"
#                 }
#             )

#         # Return success response
#         return ORJSONResponse(
#             content={
#                 "message": result,
#                 "status": "success",
#                 "recipient": email_config.recipient_email,
#                 "store_id": request.store_id
#             }
#         )

#     except HTTPException as http_exc:
#         logger.error(f"HTTP Exception: {http_exc.detail}")
#         raise http_exc
    
#     except Exception as e:
#         logger.error("Unexpected error in endpoint", exc_info=True)
#         error_message = str(e) if str(e) else "Unexpected server error"
#         raise HTTPException(
#             status_code=500,
#             detail=error_message
#         )


# ============================END OFFLINE INV =====================================================#

# API endpoint to read and process the CSV from a file path
# @app.post("/process-clients-address/", response_model=List[ClientAddressResponse])
# async def process_clients_address(db: AsyncSession = Depends(get_primary_db)):
#     # File path to the CSV
#     file_path = './csv/eposdata.csv'  # Updated to use relative path
    
#     # Check if the file exists
#     if not os.path.isfile(file_path):
#         raise HTTPException(status_code=400, detail="CSV file not found.")

#     # Read the file with ISO-8859-1 encoding
#     with open(file_path, newline='', encoding='ISO-8859-1') as csvfile:
#         csv_reader = csv.DictReader(csvfile)

#         clients_data = []

#         for row in csv_reader:
#             # Parse each row and handle missing values gracefully
#             customer = row.get("Customer Name", "").strip() or None
#             customer_number = row.get("Customer Number", "").strip() or None
#             job = row.get("Job", "").strip() or None
#             address = row.get("Address", "").strip() or None
#             postal_code = row.get("Postal Code", "").strip() or None
#             latitude = row.get("Latitude", "").strip() or None
#             longitude = row.get("Longitude", "").strip() or None

#             # Create a new instance of PosArcDHead
#             client_entry = PosArcDHead(
#                 customer=customer,
#                 customer_number=customer_number,
#                 job=job,
#                 address=address,  # Use the address without the postal code
#                 postal_code=postal_code,
#                 latitude=latitude,
#                 longitude=longitude
#             )

#             # Add to the database session
#             db.add(client_entry)

#             # Append for response
#             clients_data.append(client_entry)

#     # Commit the session to insert all records
#     try:
#         await db.commit()
#     except Exception as e:
#         await db.rollback()
#         raise HTTPException(status_code=500, detail=f"Error inserting records into the database: {str(e)}")

#     # Return the response with inserted data
#     return clients_data

# ============================ INCLUDE ROUTERS =====================================================#
# ============================ REDONDANCE =====================================================#
# @app.get("/super_redondance/ping", status_code=200)
# async def ping(db: AsyncSession = Depends(get_primary_db)):
#     """
#     Simple endpoint to check database connectivity by executing SELECT 1
#     """
#     try:
#         stmt = select(Order.id).limit(1)
#         result = await db.execute(stmt)
#         _ = result.scalar_one_or_none()
#         return {"status": "OK"}
#     except Exception as e:
#         print(f"Error in ping endpoint: {str(e)}")
#         raise HTTPException(
#             status_code=500,
#             detail="Internal server error occurred while checking database"
#         )
# ============================ INCLUDE ROUTERS =====================================================#

app.include_router(register_router)
app.include_router(driver_router)
app.include_router(driver_order_router)
app.include_router(geo_router)
app.include_router(image_router)
app.include_router(admin_router)
app.include_router(dispatch_router)
app.include_router(statement_router)
app.include_router(driver_transfer_router)