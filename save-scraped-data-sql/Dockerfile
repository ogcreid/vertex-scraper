# Dockerfile for save-scraped-data-sql service
FROM python:3.13-slim

WORKDIR /app

# Copy requirements and install dependencies
# Note: functions-framework is missing from requirements.txt but is needed
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir functions-framework==3.*

# Copy application code
COPY main.py .

# Set the function target
ENV FUNCTION_TARGET=process_scrape_entrypoint
ENV PORT=8080

# Expose the port
EXPOSE 8080

# Run the function
CMD exec functions-framework --target=${FUNCTION_TARGET} --port=${PORT} --host=0.0.0.0

