FROM apache/superset:latest

USER root

# Install required system dependencies for mysqlclient:
# - build-essential (provides gcc and build tools)
# - pkg-config (helps find the MySQL client libs)
# - default-libmysqlclient-dev (MySQL/MariaDB client development files)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        pkg-config \
        default-libmysqlclient-dev && \
    rm -rf /var/lib/apt/lists/*

# Install Python packages
RUN pip install mysqlclient pinotdb

# Set environment variables for Superset admin
ENV ADMIN_USERNAME $ADMIN_USERNAME
ENV ADMIN_EMAIL $ADMIN_EMAIL
ENV ADMIN_PASSWORD $ADMIN_PASSWORD

# Copy initialization script and config
COPY ./superset-init.sh /superset-init.sh
COPY superset_config.py /app/
ENV SUPERSET_CONFIG_PATH /app/superset_config.py

# Change back to the superset user to run the app
USER superset

ENTRYPOINT [ "/superset-init.sh" ]
