FROM eclipse-temurin:17-jdk AS base

# Install Python
RUN apt-get update && apt-get install -y python3 python3-pip python3-venv && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN python3 -m pip install --no-cache-dir -r requirements.txt --break-system-packages

# Copy project files
COPY data/ data/
COPY pipeline/ pipeline/
COPY agents/ agents/
COPY run.py .
COPY .env .env

CMD ["python3", "run.py", "all"]
