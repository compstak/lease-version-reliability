FROM python:3.9-slim

COPY . /app
WORKDIR /app

RUN pip install --upgrade pip
RUN pip install --no-cache-dir \
  -r requirements.txt

CMD ["sh", "-c", "python -m batch.main"]
