FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY data/ data/
COPY pipeline/ pipeline/
COPY agents/ agents/
COPY run.py .

CMD ["python3", "run.py", "all"]
