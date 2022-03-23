FROM python:3.9-slim-bullseye 
WORKDIR /app
COPY requirements.txt requirements.txt
RUN pip3 install --no-cache-dir -r requirements.txt
COPY crawler.py .
CMD ["python3", "-u", "/app/crawler.py"]