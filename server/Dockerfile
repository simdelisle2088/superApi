# Use a single-stage build with a lightweight Alpine base image
FROM python:3.12.3-alpine3.19

# Set the working directory in the container
WORKDIR /app

# Install build dependencies
RUN apk add --no-cache gcc musl-dev libffi-dev

# Copy only the requirements file first to leverage Docker cache
COPY requirements.txt /app/

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt || { echo "pip install failed"; cat /app/requirements.txt; exit 1; }

# Ensure uvloop is not installed
RUN pip uninstall -y uvloop || true

# Copy the files and folders to the working directory
COPY main.py /app/
COPY controller/ /app/controller/
COPY model/ /app/model/
COPY utility/ /app/utility/

# Set environment variables for configuration
ENV HOST="127.0.0.1"
# ENV PORT=8101
ENV PORT=8011
ENV LOG_LEVEL=info
ENV WORKERS=2

# Expose the port the app runs on
# EXPOSE 8101
EXPOSE 8011

# Command to run the application using Uvicorn without uvloop
CMD ["sh", "-c", "uvicorn main:app --host $HOST --port $PORT --log-level $LOG_LEVEL --workers $WORKERS"]
