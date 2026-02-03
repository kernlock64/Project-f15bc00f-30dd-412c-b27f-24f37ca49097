FROM python:3.12-slim

WORKDIR /service

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN chmod +x ./entrypoint.sh

ENTRYPOINT ["/service/entrypoint.sh"]

CMD ["sh", "-c", "uvicorn service:app --host 0.0.0.0 --port $SERVICE_PORT"]