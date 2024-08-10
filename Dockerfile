# Use the official slim Python 3.12 image as a base
FROM python:3.12-slim

# Set environment variables
ENV POETRY_VERSION=1.5.0
ENV KRB5_CONFIG=/etc/krb5.conf

# Install dependencies
RUN apt-get update && \
    apt-get install -y curl gcc libkrb5-dev && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Install Poetry
RUN curl -sSL https://install.python-poetry.org | python3 -

# Set PATH for Poetry
ENV PATH="/root/.local/bin:$PATH"

# Create and set the working directory
WORKDIR /app

# Copy the project files
COPY pyproject.toml poetry.lock ./
COPY . .

# Install Python dependencies with Poetry
RUN poetry install --no-root

# Copy the krb5.conf file
COPY krb5.conf /etc/krb5.conf

# Set the entrypoint to run the scripts
ENTRYPOINT ["poetry", "run", "python"]
