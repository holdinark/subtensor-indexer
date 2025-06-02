FROM python:3.12-slim

WORKDIR /app

COPY ./requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY ./shared /app/shared
COPY ./shovel_subnet_stats /app/shovel_subnet_stats

ENV PYTHONPATH="/app:/app/shared"

CMD ["python", "-u", "shovel_subnet_stats/main.py"]
