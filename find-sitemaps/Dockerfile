# Dockerfile for find-sitemaps service
FROM python:3.13-slim

WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY main.py .

# Set the function target
ENV FUNCTION_TARGET=find_sitemaps_for_urls_http
ENV PORT=8080

# Expose the port
EXPOSE 8080

# Run the function
CMD exec functions-framework --target=${FUNCTION_TARGET} --port=${PORT} --host=0.0.0.0

