# import csv
# import os
# from datetime import datetime
# from typing import Optional, List, Any
# from sqlalchemy.ext.asyncio import AsyncSession
# from sqlalchemy.future import select
# from pydantic import BaseModel, EmailStr, Field
# from pathlib import Path
# import logging
# import smtplib
# from email.mime.text import MIMEText
# from email.mime.multipart import MIMEMultipart
# from email.mime.application import MIMEApplication
# from model.superLocator.InvModel import InvLocations
# from dotenv import load_dotenv

# # Load environment variables
# load_dotenv()

# # Configure logging with a more detailed format
# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
# )
# logger = logging.getLogger(__name__)

# class EmailConfig(BaseModel):
#     """Configuration for email settings with validation
    
#     This class defines the email server configuration and credentials needed
#     for sending exported location data via email.
#     """
#     smtp_server: str = Field(
#         default="smtp.gmail.com",
#         description="SMTP server address"
#     )
#     smtp_port: int = Field(
#         default=587,
#         description="SMTP server port"
#     )
#     sender_email: EmailStr = Field(
#         ...,  # This means the field is required
#         description="Email address to send from"
#     )
#     app_password: str = Field(
#         ...,  # This means the field is required
#         description="App password for email authentication"
#     )
#     recipient_email: EmailStr = Field(
#         default="simon.delisle@pasuper.com",
#         description="Default recipient email address"
#     )

#     class Config:
#         from_attributes = True

# class StoreInvRequest(BaseModel):
#     """Request model for store inventory export
    
#     Contains the store identifier for which to export location data.
#     """
#     store_id: str = Field(..., description="Store identifier")

# class LocationExporter:
#     """Handles the export of store location data to CSV and email delivery
    
#     This class manages the entire process of:
#     1. Querying location data from the database
#     2. Generating CSV files
#     3. Sending emails with attachments
#     4. Cleaning up temporary files
#     """

#     def __init__(self, email_config: EmailConfig):
#         """Initialize the LocationExporter with email configuration
        
#         Args:
#             email_config: Email server and authentication configuration
#         """
#         self.email_config = email_config
        
#         # Use platform-independent temporary directory
#         temp_base = os.environ.get('TEMP', '/tmp')
#         self.temp_dir = Path(os.path.join(temp_base, 'location_exports'))
#         self.temp_dir.mkdir(exist_ok=True, parents=True)
        
#         # Define required fields for data validation
#         self.required_fields = [
#             "id", "store", "level", "row", "side", "column",
#             "shelf", "bin", "full_location"
#         ]

#     async def export_locations_by_email(self, store_id: str, db: AsyncSession) -> str:
#         """Export store locations to CSV and send via email
        
#         Args:
#             store_id: Store identifier
#             db: Database session
            
#         Returns:
#             str: Success message with details
            
#         Raises:
#             ValueError: If store_id is invalid or data is missing required fields
#             IOError: If file operations or email sending fails
#         """
#         try:
#             logger.info(f"Starting export for store_id: {store_id}")
            
#             # Validate store_id
#             if not store_id or not store_id.strip():
#                 raise ValueError("Store ID cannot be empty")
            
#             # Generate CSV file
#             local_path = await self._generate_csv(store_id, db)
#             if not local_path:
#                 return f"No locations found for store {store_id}."

#             # Send email
#             await self._send_email(local_path, store_id)
            
#             # Cleanup
#             self._cleanup_file(local_path)
            
#             return f"CSV file {local_path.name} successfully sent to {self.email_config.recipient_email}"

#         except Exception as e:
#             logger.error(f"Export failed for store {store_id}", exc_info=True)
#             raise

#     def _validate_location_data(self, location: Any) -> List[str]:
#         """Validate that a location record has all required fields
        
#         Args:
#             location: Location record to validate
            
#         Returns:
#             List of missing field names, empty if all fields present
#         """
#         return [
#             field for field in self.required_fields 
#             if getattr(location, field, None) is None
#         ]

#     async def _generate_csv(self, store_id: str, db: AsyncSession) -> Optional[Path]:
#         """Generate CSV file with location data
        
#         Args:
#             store_id: Store identifier
#             db: Database session
            
#         Returns:
#             Path to generated CSV file, or None if no data found
            
#         Raises:
#             ValueError: If data validation fails
#             IOError: If file operations fail
#         """
#         # Query locations
#         result = await db.execute(
#             select(InvLocations).where(
#                 InvLocations.store == store_id,
#                 InvLocations.is_archived == False
#             )
#         )
#         locations = result.scalars().all()

#         if not locations:
#             logger.info(f"No locations found for store {store_id}")
#             return None

#         # Validate data
#         for location in locations:
#             missing_fields = self._validate_location_data(location)
#             if missing_fields:
#                 raise ValueError(
#                     f"Location data missing required fields: {', '.join(missing_fields)}"
#                 )

#         # Generate unique filename with timestamp
#         timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
#         filename = f"locations_{store_id}_{timestamp}.csv"
#         file_path = self.temp_dir / filename

#         try:
#             with open(file_path, mode="w", newline="", encoding="utf-8") as csv_file:
#                 writer = csv.writer(csv_file)
                
#                 # Write header
#                 writer.writerow([
#                     "id", "upc", "name", "store", "level", "row", "side", "column",
#                     "shelf", "bin", "full_location", "updated_by", "updated_at",
#                     "created_by", "created_at", "is_archived"
#                 ])
                
#                 # Write data
#                 for location in locations:
#                     writer.writerow([
#                         location.id, location.upc, location.name, location.store,
#                         location.level, location.row, location.side, location.column,
#                         location.shelf, location.bin, location.full_location,
#                         location.updated_by, location.updated_at, location.created_by,
#                         location.created_at, location.is_archived
#                     ])

#             logger.info(f"CSV file created at {file_path}")
#             return file_path

#         except IOError as e:
#             logger.error(f"Failed to create CSV file: {e}", exc_info=True)
#             raise

#     async def _send_email(self, file_path: Path, store_id: str) -> None:
#         """Send email with CSV attachment
        
#         Args:
#             file_path: Path to CSV file to attach
#             store_id: Store identifier for email subject
            
#         Raises:
#             IOError: If email sending fails
#         """
#         try:
#             # Prepare email message
#             msg = MIMEMultipart()
#             msg['From'] = self.email_config.sender_email
#             msg['To'] = self.email_config.recipient_email
#             msg['Subject'] = f'Store Locations Export - Store {store_id}'

#             body = f"""
#             Voici les localisations pour le magasin {store_id}.
#             Voici les informations suivantes :
#             - Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
#             - Store: {store_id}
#             """
#             msg.attach(MIMEText(body, 'plain'))

#             # Attach CSV file
#             with open(file_path, 'rb') as file:
#                 attachment = MIMEApplication(file.read(), _subtype='csv')
#                 attachment.add_header(
#                     'Content-Disposition', 
#                     'attachment', 
#                     filename=file_path.name
#                 )
#                 msg.attach(attachment)

#             # Send email with detailed error handling
#             try:
#                 with smtplib.SMTP(self.email_config.smtp_server, self.email_config.smtp_port) as server:
#                     server.ehlo()  # Identify ourselves to the server
#                     server.starttls()  # Secure the connection
#                     server.ehlo()  # Re-identify ourselves over TLS connection
                    
#                     try:
#                         server.login(
#                             self.email_config.sender_email,
#                             self.email_config.app_password
#                         )
#                     except smtplib.SMTPAuthenticationError as auth_error:
#                         logger.error(f"Authentication failed: {auth_error}")
#                         raise IOError(
#                             "Email authentication failed. Please verify your credentials "
#                             "and ensure you're using an App Password if using Gmail."
#                         )

#                     server.send_message(msg)
#                     logger.info(f"Email sent successfully to {self.email_config.recipient_email}")

#             except smtplib.SMTPConnectError as conn_error:
#                 logger.error(f"Connection error: {conn_error}")
#                 raise IOError(f"Failed to connect to email server: {conn_error}")

#         except Exception as e:
#             logger.error(f"Failed to send email: {str(e)}")
#             raise IOError(f"Email sending failed: {str(e)}")

#     def _cleanup_file(self, file_path: Path) -> None:
#         """Clean up temporary CSV file
        
#         Args:
#             file_path: Path to file to delete
#         """
#         try:
#             file_path.unlink()
#             logger.info(f"Cleaned up temporary file: {file_path}")
#         except OSError as e:
#             logger.warning(f"Failed to cleanup file {file_path}: {e}")
