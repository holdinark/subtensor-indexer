ARG SERVICE_NAME
FROM python:3.12-slim as base

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    build-essential \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy and install Python dependencies
COPY ./requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy shared code
COPY ./shared /app/shared

# Set Python path
ENV PYTHONPATH="/app:/app/shared"

# Copy service-specific code
ARG SERVICE_NAME
ENV SERVICE_NAME=${SERVICE_NAME}
COPY ./${SERVICE_NAME} /app/${SERVICE_NAME}

# Set the default command
CMD python -u ${SERVICE_NAME}/main.py 