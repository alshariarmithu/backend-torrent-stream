FROM python:3.13-slim-trixie

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PORT=8081
EXPOSE ${PORT}

CMD ["python", "main.py"]
