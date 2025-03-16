
FROM python:3.10-slim-buster

WORKDIR /app

RUN apt-get update && apt-get install -y build-essential && rm -rf /var/lib/apt/lists/*

# Copy the lookup requirements file first so Docker can cache
COPY services/lookup/requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# Now copy the entire repo, giving us /app/services, /app/alembic, etc.
COPY . /app

ENV PYTHONPATH=/app
EXPOSE 8000

CMD ["uvicorn", "services.lookup.app_lookup:app", "--host", "0.0.0.0", "--port", "8000"]
