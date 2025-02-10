# Stage 1: Build stage
FROM python:3.12.2-alpine3.19 as builder

# Set the working directory in the container
WORKDIR /build

# Install system dependencies required for Shapely
RUN apk add --no-cache gcc musl-dev geos-dev

# Copy the requirements file into the container
COPY requirements.txt .

# Install dependencies along with uvloop and httptools for performance
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# Stage 2: Runtime stage
FROM python:3.12.2-alpine3.19

# Set the working directory in the container
WORKDIR /app

# Install runtime GEOS library
RUN apk add --no-cache geos

# Copy installed packages from the builder stage
COPY --from=builder /install /usr/local

# Copy the application source code into the container
COPY . .

# Set environment variables for configuration
ENV PORT=8001
ENV LOG_LEVEL=warning
ENV LOOP=uvloop
ENV HTTP=httptools
ENV WORKERS=3
ENV HOST="127.0.0.1"

# Expose the port the app runs on
EXPOSE 8001

# Command to run the application using Uvicorn
CMD uvicorn geo:app --host $HOST --port $PORT --loop $LOOP --http $HTTP --log-level $LOG_LEVEL --workers $WORKERS --no-use-colors --proxy-headers --no-server-header --no-date-header
