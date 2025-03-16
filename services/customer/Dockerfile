FROM python:3.10-slim-buster

WORKDIR /app

RUN apt-get update && apt-get install -y build-essential && rm -rf /var/lib/apt/lists/*

COPY services/customer/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app

ENV PYTHONPATH="/app"
EXPOSE 8000

CMD ["uvicorn", "services.customer.app_customer:app", "--host", "0.0.0.0", "--port", "8000"]
