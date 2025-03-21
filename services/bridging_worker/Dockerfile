# services/bridging_worker/Dockerfile

FROM python:3.10-slim-buster

WORKDIR /app

RUN apt-get update && apt-get install -y build-essential && rm -rf /var/lib/apt/lists/*

# 1) Copy bridging worker requirements
COPY services/bridging_worker/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# 2) Copy entire 'services' so bridging_worker can see `services.common_lib`
COPY services /app/services

ENV PYTHONPATH="/app"
