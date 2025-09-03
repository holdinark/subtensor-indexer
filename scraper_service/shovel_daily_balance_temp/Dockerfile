FROM python:3.12-slim

WORKDIR /app

COPY ./requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
RUN pip install patchelf

# Install Rust
RUN apt-get -qq update
RUN apt-get install -y -q \
  build-essential \
  curl

WORKDIR /app
COPY ./shared /app/shared
COPY ./shovel_daily_balance_temp /app/shovel_daily_balance_temp

ENV PYTHONPATH="/app:/app/shared"

CMD ["python", "-u", "shovel_daily_balance_temp/main.py"]
