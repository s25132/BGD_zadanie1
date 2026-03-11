FROM python:3.11-slim

# Ustawiamy katalog roboczy
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY pipeline.py ./ 

# Domyślny command: odpal skrypt
CMD ["python", "pipeline.py"]
