# Use a prebuilt image with libtorrent & qbittorrent installed
FROM wernight/qbittorrent:latest

# Set working directory
WORKDIR /app

# Copy Python dependencies
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the FastAPI app code
COPY . .

# Expose FastAPI port
EXPOSE 8000

# Environment variables
ENV TORRENT_DOWNLOAD_DIR=/data/torrents
ENV JWT_SECRET=change-me-in-production

# Start FastAPI with Uvicorn
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]