FROM python:3.11-slim-bookworm

WORKDIR /app

COPY . /app

ENV PYTHONPATH "${PYTHONPATH}:/app/common"

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python3", "/app/storing.py"]
