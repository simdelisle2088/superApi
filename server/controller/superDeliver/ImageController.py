from utility.util import AsyncSession, STATE_OF_DEPLOYMENT  # Importing necessary modules and constants
from fastapi.responses import ORJSONResponse  # Importing ORJSONResponse for JSON responses
from PIL import Image, ImageDraw  # Importing PIL for image processing
from sqlalchemy.future import select  # Importing select for database queries
from fastapi import Form, UploadFile  # Importing UploadFile for file uploads
from datetime import datetime  # Importing datetime for timestamping
from fastapi import status  # Importing status for HTTP status codes
import logging  # Importing logging for logging warnings
import os  # Importing os for file system operations
import io  # Importing io for in-memory file operations
from model.superDeliver.DriverOrderModel import DriverOrder  # Importing DriverOrder model

async def store_image(
    file: UploadFile,
    db: AsyncSession,
    type: str = "unknown",
):
    """
    Store an uploaded image file with a timestamp and save it to the database.

    Args:
        file (UploadFile): The uploaded image file.
        db (AsyncSession): The database session.
        type (str): The type of image, either "signatures" or "photos".
        name (str): The name to be displayed on the image.

    Returns:
        ORJSONResponse: The response indicating the result of the operation.
    """
    try:
        # Validate the type of image
        if type not in ["signatures", "photos"]:
            return ORJSONResponse(
                status_code=status.HTTP_400_BAD_REQUEST,
                content={"detail": "Mauvais type d'image."},
            )

        # Validate the filename format
        parts = file.filename.split("_")
        if len(parts) != 4:
            return ORJSONResponse(
                status_code=status.HTTP_400_BAD_REQUEST,
                content={"detail": "Le nom de fichier est incorrect."},
            )

        store, _, orderNumber, _ = parts 

        # Query the database for the driver order
        result = await db.execute(
            select(DriverOrder)
            .where(DriverOrder.store == store, DriverOrder.order_number == orderNumber)
            .limit(1)
        )
        driver = result.scalars().first()

        # Check if the driver order exists
        if not driver:
            return ORJSONResponse(
                status_code=status.HTTP_404_NOT_FOUND,
                content={"message": "Livreur non trouvé."},
            )

        # Define base paths for different deployment states
        BASE_PATHS = {
            "prod": "/home/images",
            "dev": "/home/images-dev",
            "local": "images",
        }
        path_base = BASE_PATHS.get(STATE_OF_DEPLOYMENT)
        
        if path_base is None:
            raise ValueError("Invalid STATE_OF_DEPLOYMENT")

        # Create the directory structure based on the current date
        now = datetime.now()
        directory = f"{path_base}/{type}/{now.strftime('%Y')}/{now.strftime('%m')}/{now.strftime('%d')}"
        os.makedirs(directory, exist_ok=True)

        # Define the file path and read the image data
        file_path = os.path.join(directory, file.filename)
        image_data = await file.read()

        # Open the image and draw the timestamp along with the name
        with Image.open(io.BytesIO(image_data)) as img:
            draw = ImageDraw.Draw(img)
            timestamp = now.strftime("%Y-%m-%d %H:%M:%S")
            width, height = img.size

            # Define text position
            WIDTH_OFFSET = 0.02
            HEIGHT_OFFSET = 0.93

            text_position = (
                width * WIDTH_OFFSET,
                height * HEIGHT_OFFSET,
            )

            outline_color = "black"
            outline_width = 2

            # Draw the outline by drawing the text multiple times around the text position
            for offset in [(outline_width, 0), (-outline_width, 0), (0, outline_width), (0, -outline_width),
                        (outline_width, outline_width), (outline_width, -outline_width),
                        (-outline_width, outline_width), (-outline_width, -outline_width)]:
                draw.text(
                    (text_position[0] + offset[0], text_position[1] + offset[1]), 
                    timestamp, fill=outline_color
                )

            # Draw the actual text on top of the outline
            draw.text(text_position, timestamp, fill="yellow")

            # Remove the old image file if it exists and update the database
            if (
                os.path.exists(driver.photo_filename)
                and STATE_OF_DEPLOYMENT == "prod"
            ):
                os.remove(driver.photo_filename)
            driver.photo_filename = file_path

            # Save the image and commit the transaction
            img.save(file_path)
            await db.commit()

        return ORJSONResponse(
            status_code=status.HTTP_200_OK,
            content={"message": "Image enregistrée correctement."},
        )

    except Exception as e:
        # Log the exception and return an error response
        logging.warning(f"Failed to save the image with timestamp and name: {e}")
        return ORJSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"message": "Échec de l'enregistrement de l'image."},
        )