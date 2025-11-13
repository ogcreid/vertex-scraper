# Dockerfile for vertex-admin service (Node.js)
FROM node:20-slim

WORKDIR /app

# Copy package files
COPY package*.json ./

# Install dependencies
RUN npm ci --only=production

# Copy application code
COPY server.js .
COPY public/ ./public/

# Set the port
ENV PORT=8080

# Expose the port
EXPOSE 8080

# Start the server
CMD ["node", "server.js"]

