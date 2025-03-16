# services/ingestion/Dockerfile

FROM python:3.10-slim-buster

WORKDIR /app

RUN apt-get update && apt-get install -y build-essential && rm -rf /var/lib/apt/lists/*

# 1) Copy ingestion requirements from the correct sub-path
COPY services/ingestion/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# 2) Copy Alembic config & migrations (from top-level in your project)
COPY alembic.ini /app/
COPY alembic /app/alembic

# 3) Copy entire 'services/' folder (including ingestion code + common_lib, bridging_worker, etc.)
COPY services /app/services

ENV PYTHONPATH="/app"

EXPOSE 8000

CMD ["uvicorn", "services.ingestion.app_ingestion:app", "--host", "0.0.0.0", "--port", "8000"]
