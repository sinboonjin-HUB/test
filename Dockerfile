FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends     ca-certificates tzdata &&     rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app

RUN mkdir -p /data

EXPOSE 8080

CMD ["python", "app.py"]
