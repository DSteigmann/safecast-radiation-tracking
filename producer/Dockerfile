FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY producer-requirements.txt ./
RUN pip install --no-cache-dir -r producer-requirements.txt

COPY producer.py ./
COPY start.sh ./
#COPY measurements-out-short.csv ./
RUN chmod +x start.sh

EXPOSE 8042

CMD ["./start.sh"]
