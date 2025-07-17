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
RUN curl https://sh.rustup.rs -sSf | bash -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

# Copy the rust bindings and build
WORKDIR /app
COPY ./shovel_stake_map_test_copy/rust_bindings /app/rust_bindings
WORKDIR /app/rust_bindings
RUN maturin build --release
RUN pip install /app/rust_bindings/target/wheels/*.whl

WORKDIR /app
COPY ./shared /app/shared
COPY ./shovel_stake_map_test_copy /app/shovel_stake_map_test_copy

ENV PYTHONPATH="/app:/app/shared"

CMD ["python", "-u", "shovel_stake_map_test_copy/main.py"] 