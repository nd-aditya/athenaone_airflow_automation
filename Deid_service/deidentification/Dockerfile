FROM ubuntu:22.04

# Set environment variables
ENV DEBIAN_FRONTEND=noninteractive
ENV ACCEPT_EULA=Y
ENV PIP_NO_BINARY=:none:

# Create necessary directories
RUN mkdir -p /nd_deployment

# Copy application files
COPY deIdentification /nd_deployment/deIdentification
COPY requirements.txt /nd_deployment/requirements.txt
COPY run.sh /nd_deployment/run.sh
COPY NOTEBOOK /NOTEBOOK

# Set working directory
WORKDIR /nd_deployment

# Install required system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    unixodbc \
    unixodbc-dev \
    gcc \
    g++ \
    make \
    libffi-dev \
    libssl-dev \
    libxml2-dev \
    libxslt1-dev \
    zlib1g-dev \
    libblas-dev \
    liblapack-dev \
    gfortran \
    libpq-dev \
    python3.10 \
    python3.10-dev \
    python3-pip \
    curl \
    gnupg2 \
    apt-transport-https \
    software-properties-common \
    wget

# Add Microsoft repository and install ODBC driver (updated method)
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/ubuntu/22.04/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql18 mssql-tools18 && \
    echo 'export PATH="$PATH:/opt/mssql-tools18/bin"' >> ~/.bashrc && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Add Microsoft tools to PATH
ENV PATH="/opt/mssql-tools18/bin:${PATH}"

# Upgrade pip and install Python dependencies
RUN python3.10 -m pip install --no-cache-dir --upgrade pip && \
    python3.10 -m pip install --no-cache-dir -r requirements.txt

# Install spaCy 3.5.0 and the en_core_web_lg model
RUN python3.10 -m pip install --no-cache-dir spacy==3.5.0 && \
    python3.10 -m pip install --no-cache-dir https://github.com/explosion/spacy-models/releases/download/en_core_web_lg-3.5.0/en_core_web_lg-3.5.0-py3-none-any.whl

# Ensure run.sh is executable
RUN chmod +x /nd_deployment/run.sh

# Configure Jupyter Lab password
RUN mkdir -p /root/.jupyter && \
    echo "from jupyter_server.auth.security import set_password; set_password('ND@123')" | python3.10

# Expose necessary ports
EXPOSE 8000 8888

# Set default command to run Django server
CMD ["python3.10", "deIdentification/manage.py", "runserver", "0.0.0.0:8000"]
