# Image for the ingestion API and the file-watcher
FROM python:3.11-slim
WORKDIR /opt/pharma-dwh
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
ENV PYTHONPATH=/opt/pharma-dwh PYTHONUNBUFFERED=1
