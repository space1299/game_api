FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 CMD python -c "import json, os, sys, urllib.request; req = urllib.request.Request('http://127.0.0.1:8000/health', headers={os.getenv('API_ACCESS_HEADER', 'X-API-Key'): os.getenv('API_ACCESS_KEY', '')});\
resp = urllib.request.urlopen(req, timeout=3); \
sys.exit(0 if resp.status == 200 else 1)"

CMD ["python", "-m", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
