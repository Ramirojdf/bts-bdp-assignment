FROM python:3.11-slim

WORKDIR /opt/app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY bdi_api bdi_api

ENV BDI_LOCAL_DIR=/opt/data
RUN mkdir -p /opt/data

EXPOSE 8080

CMD ["uvicorn", "bdi_api.app:app", "--proxy-headers", "--host", "0.0.0.0", "--port", "8080"]
