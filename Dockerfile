FROM python:3.11-slim

# Install libtorrent OS deps
RUN apt-get update && apt-get install -y \
    python3-libtorrent \
    libboost-python-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8080

ENV TORRENT_DOWNLOAD_DIR=/data/torrents
ENV JWT_SECRET=change-me-in-production

CMD ["python", "main.py"]
