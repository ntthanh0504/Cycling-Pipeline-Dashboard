# Base image for Kafka
FROM bitnami/kafka:2.8.0

# Set working directory
WORKDIR /app

# Temporarily switch to root user for installation
USER 0

# Install required system packages and clean up
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3 \
        python3-venv \
        python3-dev \
        python3-pip \
        gcc \
        g++ \
        make \
        librdkafka-dev \
        libsasl2-dev \
        libssl-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Copy requirements.txt into the container
COPY requirements.txt /app/

# Create a virtual environment and install dependencies
RUN python3 -m venv /app/venv && \
    /app/venv/bin/pip install --no-cache-dir wheel && \
    /app/venv/bin/pip install --no-cache-dir -r /app/requirements.txt

# Switch back to the non-root user
USER 1001

# Set the entrypoint to use the virtual environment
ENTRYPOINT ["/app/venv/bin/python"]