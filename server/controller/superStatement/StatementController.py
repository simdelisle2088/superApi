from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import logging
import os
import smtplib
from typing import List
from fastapi.responses import ORJSONResponse
from sqlalchemy import MetaData, Table, create_engine
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi.exceptions import HTTPException
from fastapi import status
from sqlalchemy.future import select
from fastapi import HTTPException
from fastapi import status
from sqlalchemy import or_, and_, select
from sqlalchemy.orm import sessionmaker
import pandas as pd
from model.QueryModel import QueryParams
from model.superDeliver.DriverOrderModel import DriverOrder
from model.superDeliver.OrderModel import Order
from model.superStatement.CustomerModel import (
    CustomerInfo, 
    CustomerInfoModel, 
    CustomerInfoResponse
)
from model.superStatement.StatementModel import (
    ClientStatement,
    ClientStatementBillCreate,
    ClientStatementBillResponse,
)
from model.superStatement.TemplatesModel import (
    HTMLContent,
    HTMLTemplate
)
from utility.util import (
    PRIMARY_DATABASE_URL,
    SS_EMAIL,
    SS_EMAIL_PASSWORD,
    BASE_PATHS_CSV,
    AsyncSession,
    STATE_OF_DEPLOYMENT
)

# async def get_all_psl(
#     store: int, db: AsyncSession, db_secondary: AsyncSession, params: QueryParams
# ):
#     try:
#         query = select(Order)

#         # Build the conditions
#         conditions = [
#                 Order.store == store,or_(
#                 Order.ship_addr1.like('%PSL%'),
#                 Order.ship_addr2.like('%PSL%'),
#                 Order.ship_addr3.like('%PSL%')
#             )]
#         if params.dateFrom:
#             conditions.append(Order.created_at >= params.dateFrom)
#         if params.dateTo:
#             conditions.append(Order.created_at <= params.dateTo)
            
#         if params.contain:
#             conditions.append(or_(
#                     Order.order_number.like(f'%{params.contain}%'),
#                     Order.client_name.like(f'%{params.contain}%'),
#                     Order.customer.like(f'%{params.contain}%'),
#                 ))

#         # Apply conditions and ordering
#         query = query.filter(and_(*conditions)).order_by(Order.created_at.desc())

#         # Apply pagination if needed
#         if params.offset:
#             query = query.offset(params.offset)
#         if params.count:
#             query = query.limit(params.count)

#         # Execute query and fetch results
#         result_orders = await db_secondary.execute(query)
#         orders = result_orders.scalars().all()
        
#         order_numbers = [order.order_number for order in orders]
#         orders_data = [
#             {
#                 "id": order.id,
#                 "order_number": order.order_number,
#                 "store": order.store,
#                 "customer": order.customer,
#                 "client_name": order.client_name,
#                 "phone_number": order.phone_number,
#                 "order_info": order.order_info,
#                 "pickers": order.pickers,
#                 "dispatch": order.dispatch,
#                 "drivers": order.drivers,
#                 "created_at": order.created_at,
#                 "updated_at": order.updated_at
#             }
#             for order in orders
#         ]

#         query = (
#             select(
#                 DriverOrder.price,
#                 DriverOrder.order_number,
#                 DriverOrder.driver_name,
#                 DriverOrder.delivered_at
#             ).where(DriverOrder.order_number.in_(order_numbers))
#         )

#         result = await db.execute(query)
#         driver_orders = result.fetchall()

#         # Convert fetched data into a dictionary keyed by order_number
#         driver_orders_map = {row.order_number: row for row in driver_orders}

#         # Update orders_data with matching driver information
#         for order in orders_data:
#             driver_order = driver_orders_map.get(order["order_number"])
#             if driver_order:
#                 order["price"] = driver_order.price
#                 order["driver_name"] = driver_order.driver_name
#                 order["delivered_at"] = driver_order.delivered_at

#         return ORJSONResponse(
#             status_code=status.HTTP_200_OK,
#             content={"detail": orders_data},
#         )
#     except Exception as e:
#         logging.error(f"Error fetching all orders: {e}", exc_info=True)
#         return ORJSONResponse(
#             status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
#             content={"detail": "Unknown error occurred while retrieving orders."},
#         )

async def fetch_statements(session: AsyncSession) -> List[ClientStatementBillResponse]:
    result = await session.execute(select(ClientStatement))
    statements = result.scalars().all()
    return [ClientStatementBillResponse.from_orm(statement).dict() for statement in statements]

async def fetch_html_template(file_name: str, session: AsyncSession):
    result = await session.execute(select(HTMLTemplate).filter(HTMLTemplate.name == file_name))
    return result.scalars().first()

async def save_html_template(content: HTMLContent, session: AsyncSession):
    result = await session.execute(select(HTMLTemplate).filter(HTMLTemplate.name == content.fileName))
    html_template = result.scalars().first()
    if html_template:
        html_template.content = content.html
    else:
        html_template = HTMLTemplate(name=content.fileName, content=content.html)
        session.add(html_template)
    await session.commit()
    await session.refresh(html_template)
    return {"message": "File saved successfully"}

def execute_customer_script():
    csv_path = BASE_PATHS_CSV.get(STATE_OF_DEPLOYMENT)
    if not csv_path:
        raise ValueError("CSV path not found for the current deployment state")
    
    csv_file = os.path.join(csv_path, 'customers.csv')  # Adjust the file name as necessary
    csv_db_path = PRIMARY_DATABASE_URL 
    if not csv_db_path:
        raise ValueError("Database path not found for the current deployment state")

    if not os.path.exists(csv_file):
        raise FileNotFoundError(f"CSV file not found: {csv_file}")

    df = pd.read_csv(csv_file)

    engine = create_engine(PRIMARY_DATABASE_URL)
    Session = sessionmaker(bind=engine)
    session = Session()

    metadata = MetaData()
    customer_info = Table('customer_info', metadata, autoload_with=engine)

    with engine.begin() as connection:
        for index, row in df.iterrows():
            select_stmt = select(customer_info.c.customer_number).where(customer_info.c.customer_number == row['Customer Number'])
            result = connection.execute(select_stmt).fetchone()

            if result:
                update_stmt = customer_info.update().where(customer_info.c.customer_number == row['Customer Number']).values(
                    customer_name=row['Customer Name'],
                    customer_number=row['Customer Number'],
                    running_balance=row['Running Balance'],
                    sequence=row['Sequence'],
                    email_address=row['Email Address'],
                )
                connection.execute(update_stmt)
            else:
                insert_stmt = customer_info.insert().values(
                    customer_name=row['Customer Name'],
                    customer_number=row['Customer Number'],
                    running_balance=row['Running Balance'],
                    sequence=row['Sequence'],
                    email_address=row['Email Address'],
                )
                connection.execute(insert_stmt)

    session.commit()
    session.close()

def fetch_customer_data_from_db():

    engine = create_engine(PRIMARY_DATABASE_URL)
    metadata = MetaData()
    customer_info = Table('customer_info', metadata, autoload_with=engine)

    with engine.connect() as connection:
        select_stmt = select(
            customer_info.c.customer_number,
            customer_info.c.customer_name,
            customer_info.c.running_balance,
            customer_info.c.email_address,
            customer_info.c.ready,
            customer_info.c.sent,
            customer_info.c.follow_up
        ).where(customer_info.c.running_balance > 0)
        result = connection.execute(select_stmt)
        customers = [dict(row._mapping) for row in result]

    return customers

async def create_client(data: CustomerInfoModel, db: AsyncSession) -> CustomerInfoResponse:
    normalized_customer_number = str(int(data.customer_number))

    async with db.begin():
        result = await db.execute(
            select(CustomerInfo).filter(CustomerInfo.customer_number == normalized_customer_number)
        )
        existing_user = result.scalars().first()
        if existing_user:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Customer number already exists",
            )
        
        new_user = CustomerInfo(
            customer_number=normalized_customer_number,
            customer_name=data.customer_name,
            running_balance=data.running_balance,
            sequence=data.sequence,
            email_address=data.email_address,
            ready=data.ready,
            sent=data.sent,
            follow_up=data.follow_up
        )
        
        db.add(new_user)
        await db.commit()
        
        return CustomerInfoResponse.from_orm(new_user)

async def update_client(customer_number: str, data: CustomerInfoModel, db: AsyncSession) -> CustomerInfoResponse:

    async with db.begin():
        result = await db.execute(
            select(CustomerInfo).filter(CustomerInfo.customer_number == customer_number)
        )
        existing_user = result.scalars().first()
        if not existing_user:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Customer not found",
            )

        # Update the customer's information
        existing_user.customer_name = data.customer_name
        existing_user.running_balance = data.running_balance
        existing_user.sequence = data.sequence
        existing_user.email_address = data.email_address
        existing_user.ready = data.ready
        existing_user.sent = data.sent
        existing_user.follow_up = data.follow_up
        
        await db.commit()
        
        return CustomerInfoResponse.from_orm(existing_user)

def send_email_function(to_email: str, subject: str, html_content: str):

    from_email = SS_EMAIL
    password = SS_EMAIL_PASSWORD

    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = from_email
    msg['To'] = to_email
    part = MIMEText(html_content, 'html')
    msg.attach(part)

    try:
        with smtplib.SMTP('smtp.office365.com', 587) as server:
            server.starttls()  # Upgrade the connection to TLS
            server.login(from_email, password)
            server.sendmail(from_email, to_email, msg.as_string())
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to send email: {e}")

async def process_send_email(data: ClientStatementBillCreate, db: AsyncSession):

    template_name = data.template
    if not template_name.endswith(".html"):
        template_name += ".html"

    try:
        # Fetch the HTML template from the database
        result = await db.execute(select(HTMLTemplate).where(HTMLTemplate.name == template_name))
        html_template = result.scalars().first()

        if not html_template:
            raise HTTPException(status_code=404, detail="Template not found in database")
        
        template_content = html_template.content
        email_subject = "Your Email Subject"

        # Fetch customer info from the database
        result = await db.execute(
            select(CustomerInfo).where(CustomerInfo.customer_number == data.client_number)
        )
        client_statement = result.scalars().first()

        if not client_statement:
            raise HTTPException(status_code=404, detail="Client statement not found")

        recipient_list = client_statement.email_address.split(' ')

        for recipient in recipient_list:
            recipient = recipient.strip()  # Remove any extra spaces
            send_email_function(recipient, email_subject, template_content)

        if "FollowupTemplate" in template_name:
            client_statement.follow_up = True
        elif "StatementTemplate" in template_name:
            client_statement.sent = True

        await db.commit()

    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=500, detail=f"Failed to process request: {e}")