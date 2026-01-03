# -------------------------------
# Base Image
# -------------------------------
FROM python:3.11-slim

# -------------------------------
# Environment Variables
# -------------------------------
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# -------------------------------
# Set Working Directory
# -------------------------------
WORKDIR /app

# -------------------------------
# System Dependencies
# -------------------------------
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# -------------------------------
# Install Python Dependencies
# -------------------------------
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

# -------------------------------
# Copy Application Code
# -------------------------------
COPY . .

# -------------------------------
# Default Command
# -------------------------------
CMD ["python", "scripts/pipeline_orchestrator.py"]
