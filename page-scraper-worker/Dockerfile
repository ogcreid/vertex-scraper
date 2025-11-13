# Dockerfile for page-scraper-worker service
FROM python:3.13-slim

WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY main.py .

# Set the function target
ENV FUNCTION_TARGET=page_scraper_worker
ENV PORT=8080

# Expose the port
EXPOSE 8080

# Run the function (cloud_event handler)
CMD exec functions-framework --target=${FUNCTION_TARGET} --port=${PORT} --host=0.0.0.0 --signature-type=cloudevent

