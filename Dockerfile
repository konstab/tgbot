FROM python:3.12-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1

# сертификаты, чтобы httpx нормально ходил в интернет
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY . /app

CMD ["python", "main.py"]
