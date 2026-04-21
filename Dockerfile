FROM python:3.13-slim-trixie

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/usr/lib/python3/dist-packages

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        python3-libtorrent \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && python -c "import libtorrent; print('libtorrent ready')" \
    && python -m playwright install --with-deps chromium

COPY . .

ENV PORT=8081
EXPOSE ${PORT}

CMD ["python", "main.py"]
