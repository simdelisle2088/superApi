from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import os
import re
import smtplib
from fastapi import HTTPException, encoders
from azure.identity.aio import ClientSecretCredential
from msgraph import GraphServiceClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from msgraph.generated.users.item.messages.messages_request_builder import MessagesRequestBuilder
from msgraph.generated.models.message import Message
from dotenv import load_dotenv

from utility.util import SS_EMAIL, SS_EMAIL_PASSWORD

load_dotenv()

# Azure AD app details
USERNAME = "statement@pasuper.com"
CLIENT_ID = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')
TENANT_ID = os.getenv('TENANT_ID')

def GraphCredential():
    # Create a GraphClient instance         
    credential = ClientSecretCredential(
        tenant_id=TENANT_ID,
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
    )
    
    return GraphServiceClient(credentials=credential, scopes=["https://graph.microsoft.com/.default"])

def message_to_dict(message: Message):
    return {
        "id": message.id,
        "subject": message.subject,
        "body": message.body.content,
        "additionalData": message.additional_data,
        "sender": message.sender.email_address.address if message.sender and message.sender.email_address else None,
        "receivedDateTime": message.received_date_time.isoformat() if message.received_date_time else None,
    }

def send_email(to_email: str, subject: str, html_content: str, attachment: str = None):#DUPLICATE OF StatementController.py
    from_email = SS_EMAIL
    password = SS_EMAIL_PASSWORD

    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = from_email
    msg['To'] = to_email

    # Attach the HTML content
    part = MIMEText(html_content, 'html')
    msg.attach(part)

    # Handle attachment
    if attachment:
        try:
            with open(attachment, 'rb') as f:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(f.read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', f'attachment; filename="{attachment}"')
            msg.attach(part)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to read attachment: {e}")

    # Send the email

    try:
        with smtplib.SMTP('smtp.office365.com', 587) as server:
            server.starttls()  # Upgrade the connection to TLS
            server.login(from_email, password)
            server.sendmail(from_email, to_email, msg.as_string())
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to send email: {e}")

async def check_email(db: AsyncSession):
    client = GraphCredential()

    try:
        query_params = MessagesRequestBuilder.MessagesRequestBuilderGetQueryParameters(
            select=['id', 'sender', 'subject', 'body', 'hasAttachments', 'receivedDateTime'],
            top=25, 
            orderby=['receivedDateTime DESC']
        )
        request_config = MessagesRequestBuilder.MessagesRequestBuilderGetRequestConfiguration(
            query_parameters=query_params
        )
        #gather attachment from mail
        attachments = await messages_request_builder.by_message_id(message.id).attachments.get()
        print(str(attachments.value[0].content_bytes.decode("utf-8")))
        #verify if the user exists in the db and get the email address
        user_request_builder = client.users.by_user_id(USERNAME)
        messages_request_builder = user_request_builder.messages
        result = await messages_request_builder.get(request_configuration=request_config)
        query = text("SELECT * FROM customer_info WHERE customer_name LIKE :customer_name")
        if result and result.value:
            messages = [message_to_dict(message) for message in result.value]
            for message in result.value:
                if(message.has_attachments):
                    # find name using regex
                    name = re.search(r"Dear (.*?),", message.body.content).group(1)
                    print("finding ", name, "..")
                    name = "VENTES WEB"#REMOVE TEST REMOVE TEST REMOVE TEST REMOVE TEST REMOVE TEST REMOVE TEST REMOVE TEST REMOVE TEST REMOVE TEST 
                    # find email associated with email
                    result = await db.execute(query, {'customer_name': f'%{name}%'})
                    rows = result.fetchall()
                    print(rows)
                    for customers in rows:
                        print(customers.email_address)
                        emails = customers.email_address.split(' ')#multiple emails on one line at times
                        for email in emails:
                            send_email(email, message.subject, message.body, attachments.value[0])
                # print(str(message.body))
            return messages
        else:
            return {"emails": []}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))