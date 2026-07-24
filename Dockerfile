FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt

COPY . ./

ENV PYTHONUNBUFFERED=1

EXPOSE 8081

CMD ["python", "-m", "proxy_ace.proxy_ace"]
